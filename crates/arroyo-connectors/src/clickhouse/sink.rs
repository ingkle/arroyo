use arrow::array::RecordBatch;
use arroyo_formats::ser::ArrowSerializer;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::DataflowResult;
use arroyo_rpc::grpc::rpc::TableConfig;
use arroyo_rpc::ControlResp;
use arroyo_state::global_table_config;
use arroyo_types::CheckpointBarrier;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};

const MAX_RETRIES: u32 = 20;

pub struct ClickHouseSinkFunc {
    pub insert_url: String,
    pub client: reqwest::Client,
    pub batch_size: u32,
    pub flush_interval_ms: u64,
    pub serializer: ArrowSerializer,
    pub cmd_q: Option<(Sender<u32>, Receiver<ClickHouseCmd>)>,
    pub rx: Receiver<u32>,
    pub tx: Sender<ClickHouseCmd>,
}

pub enum ClickHouseCmd {
    Data(Vec<u8>),
    Flush(u32),
}

impl ClickHouseSinkFunc {
    pub fn new(
        insert_url: String,
        client: reqwest::Client,
        batch_size: u32,
        flush_interval_ms: u64,
        serializer: ArrowSerializer,
    ) -> Self {
        let (data_tx, data_rx) = tokio::sync::mpsc::channel(4096);
        let (ack_tx, ack_rx) = tokio::sync::mpsc::channel(16);

        Self {
            insert_url,
            client,
            batch_size,
            flush_interval_ms,
            serializer,
            cmd_q: Some((ack_tx, data_rx)),
            rx: ack_rx,
            tx: data_tx,
        }
    }
}

struct ClickHouseWriter {
    rx: Receiver<ClickHouseCmd>,
    tx: Sender<u32>,
    client: reqwest::Client,
    insert_url: String,
    buffer: Vec<u8>,
    row_count: u32,
    batch_size: u32,
    flush_interval: Duration,
    last_flushed: Instant,
    operator_id: String,
    task_index: usize,
    node_id: u32,
    control_tx: tokio::sync::mpsc::Sender<ControlResp>,
}

impl ClickHouseWriter {
    fn start(mut self) {
        tokio::spawn(async move {
            loop {
                let flush_duration = self.flush_interval.checked_sub(self.last_flushed.elapsed());

                if self.row_count >= self.batch_size || flush_duration.is_none() {
                    self.flush().await;
                    continue;
                }

                let flush_timeout = tokio::time::sleep(flush_duration.unwrap());

                select! {
                    cmd = self.rx.recv() => {
                        match cmd {
                            None => {
                                info!("closing ClickHouse writer");
                                self.flush().await;
                                return;
                            }
                            Some(ClickHouseCmd::Data(data)) => {
                                self.buffer.extend_from_slice(&data);
                                self.buffer.push(b'\n');
                                self.row_count += 1;
                            }
                            Some(ClickHouseCmd::Flush(epoch)) => {
                                self.flush().await;
                                if self.tx.send(epoch).await.is_err() {
                                    info!("receiver hung up, closing ClickHouse writer");
                                    return;
                                }
                            }
                        }
                    }
                    _ = flush_timeout => {
                        self.flush().await;
                    }
                }
            }
        });
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            self.last_flushed = Instant::now();
            return;
        }

        let mut attempts = 0;
        let body = bytes::Bytes::from(std::mem::take(&mut self.buffer));
        let count = self.row_count;
        self.row_count = 0;

        while attempts < MAX_RETRIES {
            match self
                .client
                .post(&self.insert_url)
                .body(body.clone())
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    self.last_flushed = Instant::now();
                    info!("Flushed {count} rows to ClickHouse");
                    return;
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body_text = resp.text().await.unwrap_or_default();
                    warn!(
                        "ClickHouse returned {status} (attempt {attempts}): {body_text}",
                    );

                    let _ = self
                        .control_tx
                        .send(ControlResp::Error {
                            node_id: self.node_id,
                            operator_id: self.operator_id.clone(),
                            task_index: self.task_index,
                            message: format!("ClickHouse insert failed (retry {attempts})"),
                            details: format!("HTTP {status}: {body_text}"),
                        })
                        .await;
                }
                Err(e) => {
                    warn!("ClickHouse request error (attempt {attempts}): {e:?}");

                    let _ = self
                        .control_tx
                        .send(ControlResp::Error {
                            node_id: self.node_id,
                            operator_id: self.operator_id.clone(),
                            task_index: self.task_index,
                            message: format!("ClickHouse insert failed (retry {attempts})"),
                            details: e.to_string(),
                        })
                        .await;
                }
            }

            attempts += 1;
            tokio::time::sleep(Duration::from_millis((50 * (1 << attempts)).min(5_000))).await;
        }

        panic!("Exhausted {MAX_RETRIES} retries writing to ClickHouse");
    }
}

#[async_trait]
impl ArrowOperator for ClickHouseSinkFunc {
    fn name(&self) -> String {
        "ClickHouseSink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("s", "clickhouse sink state")
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let (tx, rx) = self.cmd_q.take().expect("on_start called multiple times!");

        ClickHouseWriter {
            rx,
            tx,
            client: self.client.clone(),
            insert_url: self.insert_url.clone(),
            buffer: Vec::with_capacity(64 * 1024),
            row_count: 0,
            batch_size: self.batch_size,
            flush_interval: Duration::from_millis(self.flush_interval_ms),
            last_flushed: Instant::now(),
            operator_id: ctx.task_info.operator_id.clone(),
            task_index: ctx.task_info.task_index as usize,
            node_id: ctx.task_info.node_id,
            control_tx: ctx.control_tx.clone(),
        }
        .start();

        info!(
            "ClickHouse sink started (batch_size={}, flush_interval={}ms)",
            self.batch_size, self.flush_interval_ms
        );

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        for value in self.serializer.serialize(&batch) {
            self.tx
                .send(ClickHouseCmd::Data(value))
                .await
                .expect("ClickHouse writer panicked");
        }
        Ok(())
    }

    async fn handle_checkpoint(
        &mut self,
        checkpoint: CheckpointBarrier,
        _ctx: &mut OperatorContext,
        _: &mut dyn Collector,
    ) -> DataflowResult<()> {
        self.tx
            .send(ClickHouseCmd::Flush(checkpoint.epoch))
            .await
            .expect("ClickHouse writer panicked");

        loop {
            match tokio::time::timeout(Duration::from_secs(60), self.rx.recv()).await {
                Ok(Some(epoch)) => {
                    if checkpoint.epoch == epoch {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    panic!("ClickHouse writer closed unexpectedly");
                }
                Err(_) => {
                    panic!("Timed out waiting for ClickHouse writer to confirm checkpoint flush");
                }
            }
        }
    }
}
