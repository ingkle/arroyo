mod checkpoint;
pub mod migration;
mod open_file;
mod uploads;

use super::delta::{load_or_create_table, write_batches_to_delta};
use super::parquet::representitive_timestamp;
use super::partitioning::{Partitioner, PartitionerMode};
use super::v2::checkpoint::{FileToCommit, FilesCheckpointV2};
use super::v2::uploads::{FsResponse, UploadFuture};
use super::{
    BatchBufferingWriter, CommitState, DeltaTableEntry, FinishedFile, FsEventLogger, RollingPolicy,
    add_suffix_prefix, map_storage_error,
};
use crate::filesystem::TableFormat;
use crate::filesystem::config::{self, FilenameStrategy, NamingConfig};
use crate::filesystem::sink::two_phase_committer::CommitStrategy;
use crate::filesystem::sink::v2::open_file::{CommitPreparation, OpenFile, PendingSingleFile};
use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use arroyo_operator::context::{Collector, OperatorContext};
use arroyo_operator::operator::ArrowOperator;
use arroyo_rpc::errors::{DataflowError, DataflowResult, StateError};
use arroyo_rpc::grpc::rpc::{GlobalKeyedTableConfig, TableConfig, TableEnum};
use arroyo_rpc::{CheckpointEvent, connector_err, df::ArroyoSchemaRef, formats::Format};
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_storage::StorageProvider;
use arroyo_types::{CheckpointBarrier, TaskInfo, Watermark};
use async_trait::async_trait;
use bincode::{Decode, Encode, config as bincode_config};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use object_store::path::Path;
use prost::Message;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::{env, fs, mem};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use ulid::Ulid;
use uuid::Uuid;

const DEFAULT_TARGET_PART_SIZE: usize = 32 * 1024 * 1024;

/// Serializable container for Delta batch data passed through two-phase commit.
#[derive(Encode, Decode)]
struct DeltaBatchCommitData {
    table_name: Option<String>,
    ipc_data: Vec<u8>,
}

fn encode_batch_to_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    buf
}

fn decode_batch_from_ipc(ipc_data: &[u8]) -> RecordBatch {
    let reader = arrow::ipc::reader::StreamReader::try_new(
        std::io::Cursor::new(ipc_data),
        None,
    )
    .unwrap();
    reader.into_iter().next().unwrap().unwrap()
}

pub struct ActiveState<BBW: BatchBufferingWriter + 'static> {
    max_file_index: usize,
    // partition -> filename
    active_partitions: HashMap<Option<OwnedRow>, Arc<Path>>,
    // filename -> file writer
    open_files: HashMap<Arc<Path>, OpenFile<BBW>>,
    // single files pending local commit (PerSubtask strategy)
    pending_local_commits: Vec<PendingSingleFile>,
    // Delta WriteBuilder: buffered batches between checkpoints
    // (table_name, batch) — table_name is Some for DeltaLakeMulti, None for DeltaLake
    delta_batch_buffer: Vec<(Option<String>, RecordBatch)>,
}

pub struct SinkConfig {
    config: config::FileSystemSink,
    format: Format,
    file_naming: NamingConfig,
    partitioner_mode: PartitionerMode,
    rolling_policies: Vec<RollingPolicy>,
    // consumed on start
    table_format: Option<TableFormat>,
    table_column: Option<String>,
}

impl SinkConfig {
    fn target_part_size(&self) -> usize {
        self.config
            .multipart
            .target_part_size_bytes
            .map(|s| s as usize)
            .unwrap_or(DEFAULT_TARGET_PART_SIZE)
    }

    fn minimum_multipart_size(&self) -> usize {
        self.config
            .multipart
            .minimum_multipart_size
            .unwrap_or_default() as usize
    }
}

pub struct SinkContext {
    storage_provider: Arc<StorageProvider>,
    partitioner: Arc<Partitioner>,
    schema: ArroyoSchemaRef,
    iceberg_schema: Option<iceberg::spec::SchemaRef>,
    task_info: Arc<TaskInfo>,
    commit_state: CommitState,
    table_column_index: Option<usize>,
    partition_fields: Vec<String>,
}

/// test utility to precisely cause failures
fn maybe_cause_failure(test_case: &str) {
    if !cfg!(debug_assertions) {
        return;
    }

    if env::var("FS_FAILURE_TESTING").is_ok()
        && fs::read("/tmp/fail")
            .unwrap_or_default()
            .starts_with(test_case.as_bytes())
    {
        panic!("intentionally failing due to {test_case}");
    }
}

impl SinkContext {
    pub async fn new(
        config: &mut SinkConfig,
        schema: ArroyoSchemaRef,
        task_info: Arc<TaskInfo>,
    ) -> DataflowResult<Self> {
        let mut table_format = config
            .table_format
            .take()
            .expect("table format has already been consumed");

        let provider = table_format
            .get_storage_provider(
                task_info.clone(),
                &config.config,
                &schema.schema_without_timestamp(),
            )
            .await?;

        let mut iceberg_schema = None;

        let table_column_index = config.table_column.as_ref().map(|col_name| {
            schema
                .schema
                .index_of(col_name)
                .unwrap_or_else(|_| panic!("table_column '{col_name}' not found in schema"))
        });

        let has_table_column = config.table_column.is_some();
        let commit_state = match (table_format, has_table_column) {
            (TableFormat::Delta, true) => CommitState::DeltaLakeMulti {
                tables: HashMap::new(),
                base_path: config.config.path.trim_end_matches('/').to_string(),
                storage_options: config.config.storage_options.clone(),
                schema: schema.schema_without_timestamp(),
            },
            (TableFormat::Iceberg(_), true) => {
                return Err(connector_err!(
                    User,
                    NoRetry,
                    "table_column is not supported for Iceberg sinks. \
                     Iceberg requires a fixed table location."
                ));
            }
            (TableFormat::Delta, false) => CommitState::DeltaLake {
                last_version: -1,
                table: Box::new(
                    load_or_create_table(&provider, &schema.schema_without_timestamp(), config.config.partitioning.fields.clone())
                        .await
                        .map_err(|e| {
                            connector_err!(
                                User,
                                NoRetry,
                                source: e,
                                "failed to load or create delta table"
                            )
                        })?,
                ),
            },
            (TableFormat::Iceberg(mut table), false) => {
                let t = table
                    .load_or_create(task_info.clone(), &schema.schema)
                    .await?;
                iceberg_schema = Some(t.metadata().current_schema().clone());
                CommitState::Iceberg(table)
            }
            (TableFormat::None, _) => CommitState::VanillaParquet,
        };

        let partition_fields = config.config.partitioning.fields.clone();

        Ok(SinkContext {
            storage_provider: Arc::new(provider),
            partitioner: Arc::new(Partitioner::new(
                config.partitioner_mode.clone(),
                &schema.schema,
            )?),
            schema,
            iceberg_schema,
            task_info: task_info.clone(),
            commit_state,
            table_column_index,
            partition_fields,
        })
    }
}

impl<BBW: BatchBufferingWriter> ActiveState<BBW> {
    fn next_file_path(
        &mut self,
        config: &SinkConfig,
        context: &SinkContext,
        partition: &Option<OwnedRow>,
        table_prefix: Option<&str>,
    ) -> String {
        let filename_strategy = config.file_naming.strategy.unwrap_or_default();

        // This forms the base for naming files depending on strategy
        let filename_base = match filename_strategy {
            FilenameStrategy::Serial => {
                format!(
                    "{:>05}-{:>03}",
                    self.max_file_index, context.task_info.task_index
                )
            }
            FilenameStrategy::Ulid => Ulid::new().to_string(),
            FilenameStrategy::Uuid => Uuid::new_v4().to_string(),
            FilenameStrategy::UuidV7 => Uuid::now_v7().to_string(),
        };

        let mut filename = add_suffix_prefix(
            filename_base,
            config.file_naming.prefix.as_ref(),
            config.file_naming.suffix.as_ref().unwrap(),
        );

        if let Some(partition) = partition
            && let Some(hive) = context.partitioner.hive_path(partition)
        {
            filename = format!("{hive}/{filename}");
        }

        // For multi-table routing, prepend the table name as a subdirectory
        if let Some(table) = table_prefix {
            filename = format!("{table}/{filename}");
        }

        filename
    }

    fn get_or_create_file(
        &mut self,
        config: &SinkConfig,
        logger: FsEventLogger,
        context: &SinkContext,
        partition: &Option<OwnedRow>,
        representative_ts: SystemTime,
        table_prefix: Option<&str>,
    ) -> &mut OpenFile<BBW> {
        let file = self
            .active_partitions
            .get(partition)
            .and_then(|f| self.open_files.get(f));

        if file.is_none() || !file.unwrap().is_writable() {
            let file_path = self.next_file_path(config, context, partition, table_prefix);
            let path = Arc::new(Path::from(file_path));

            let batch_writer = BBW::new(
                &config.config,
                config.format.clone(),
                context.schema.clone(),
                context.iceberg_schema.clone(),
                logger.clone(),
            );

            let open_file = OpenFile::new(
                path.clone(),
                batch_writer,
                logger,
                context.storage_provider.clone(),
                representative_ts,
                config,
            );

            self.active_partitions
                .insert(partition.clone(), path.clone());
            self.open_files.insert(path, open_file);

            self.max_file_index += 1;
        }

        let file_path = self.active_partitions.get(partition).unwrap();
        self.open_files.get_mut(file_path).unwrap()
    }
}

pub struct FileSystemSinkV2<BBW: BatchBufferingWriter + 'static> {
    config: SinkConfig,

    context: Option<SinkContext>,

    // state
    active: ActiveState<BBW>,
    upload: UploadState,

    event_logger: FsEventLogger,
    watermark: Option<SystemTime>,
}

struct UploadState {
    pending_uploads: Arc<Mutex<FuturesUnordered<UploadFuture>>>,
    files_to_commit: Vec<FileToCommit>,
}

impl UploadState {
    async fn roll_file_if_ready<BBW: BatchBufferingWriter>(
        &mut self,
        policies: &[RollingPolicy],
        watermark: Option<SystemTime>,
        f: &mut OpenFile<BBW>,
    ) -> DataflowResult<bool> {
        Ok(
            if f.is_writable() && policies.iter().any(|p| p.should_roll(&f.stats, watermark)) {
                let futures = f.close()?;

                let mut ps = self.pending_uploads.lock().await;
                ps.extend(futures.into_iter());
                true
            } else {
                false
            },
        )
    }
}

impl<BBW: BatchBufferingWriter + Send + 'static> FileSystemSinkV2<BBW> {
    pub fn new(
        config: config::FileSystemSink,
        table_format: TableFormat,
        format: Format,
        partitioner_mode: PartitionerMode,
        connection_id: Option<String>,
        table_column: Option<String>,
    ) -> Self {
        let mut file_naming = config.file_naming.clone();
        if file_naming.suffix.is_none() {
            file_naming.suffix = Some(BBW::suffix_for_format(&format).to_owned());
        }

        let connection_id_str = connection_id.clone().unwrap_or_default();
        let output_format = format.name();
        let table_format_name = table_format.name();

        Self {
            config: SinkConfig {
                rolling_policies: RollingPolicy::from_file_settings(&config),
                config,
                format,
                table_format: Some(table_format),
                file_naming,
                partitioner_mode,
                table_column,
            },
            context: None,
            active: ActiveState {
                active_partitions: HashMap::new(),
                open_files: HashMap::new(),
                max_file_index: 0,
                pending_local_commits: vec![],
                delta_batch_buffer: Vec::new(),
            },
            upload: UploadState {
                pending_uploads: Arc::new(Mutex::new(FuturesUnordered::new())),
                files_to_commit: Vec::new(),
            },
            event_logger: FsEventLogger {
                task_info: None,
                connection_id: connection_id_str.into(),
                output_format,
                table_format: table_format_name,
            },
            watermark: None,
        }
    }

    fn is_delta_write_mode(&self) -> bool {
        matches!(
            self.context.as_ref().unwrap().commit_state,
            CommitState::DeltaLake { .. } | CommitState::DeltaLakeMulti { .. }
        )
    }

    fn commit_strategy(&self) -> CommitStrategy {
        match self.context.as_ref().unwrap().commit_state {
            CommitState::DeltaLake { .. }
            | CommitState::DeltaLakeMulti { .. }
            | CommitState::Iceberg(_) => CommitStrategy::PerOperator,
            CommitState::VanillaParquet => CommitStrategy::PerSubtask,
        }
    }

    async fn process_upload_result(&mut self, result: FsResponse) -> DataflowResult<()> {
        let Some(file) = self.active.open_files.get_mut(&result.path) else {
            warn!("received multipart init for unknown file: {}", result.path);
            return Ok(());
        };

        let futures = self.upload.pending_uploads.lock().await;
        for future in file.handle_event(result.data)? {
            futures.push(future);
        }

        Ok(())
    }

    async fn flush_pending_uploads(&mut self) -> DataflowResult<()> {
        let mut count = 0;
        loop {
            let result = {
                let mut uploads = self.upload.pending_uploads.lock().await;
                if uploads.is_empty() {
                    info!("flush_pending_uploads: completed after {} uploads", count);
                    break;
                }
                debug!(
                    "flush_pending_uploads: waiting for upload (have {} pending)",
                    uploads.len()
                );
                uploads.next().await
            };

            if let Some(result) = result {
                count += 1;
                self.process_upload_result(result?).await?;
            }
        }
        Ok(())
    }

    /// Get the delta version if using Delta Lake.
    fn delta_version(&self) -> i64 {
        match &self.context.as_ref().unwrap().commit_state {
            CommitState::DeltaLake { last_version, .. } => *last_version,
            CommitState::DeltaLakeMulti { .. } => -1, // multi-table tracks versions per table
            _ => -1,
        }
    }

    async fn process_batch_inner(
        &mut self,
        batch: RecordBatch,
        timestamp_index: usize,
        table_prefix: Option<&str>,
    ) -> DataflowResult<()> {
        let partitioner = &self.context.as_ref().unwrap().partitioner;

        let partitions: Vec<(Option<OwnedRow>, RecordBatch)> = if partitioner.is_partitioned() {
            partitioner
                .partition(&batch)?
                .into_iter()
                .map(|(k, b)| (Some(k), b))
                .collect()
        } else {
            vec![(None, batch)]
        };

        for (partition_key, sub_batch) in partitions {
            let representative_timestamp =
                representitive_timestamp(sub_batch.column(timestamp_index)).map_err(|e| {
                    connector_err!(
                        Internal,
                        NoRetry,
                        source: e,
                        "failed to get representative timestamp"
                    )
                })?;

            let file = self.active.get_or_create_file(
                &self.config,
                self.event_logger.clone(),
                self.context.as_ref().unwrap(),
                &partition_key,
                representative_timestamp,
                table_prefix,
            );
            let future = file.add_batch(&sub_batch)?;

            if let Some(future) = future {
                let futures = self.upload.pending_uploads.lock().await;
                futures.push(future);
            }

            if self
                .upload
                .roll_file_if_ready(&self.config.rolling_policies, self.watermark, file)
                .await?
            {
                self.active.active_partitions.remove(&partition_key);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<BBW: BatchBufferingWriter + Send + 'static> ArrowOperator for FileSystemSinkV2<BBW> {
    fn name(&self) -> String {
        "filesystem_sink".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        let mut tables = arroyo_state::global_table_config_with_version("r", "recovery data", 1);
        tables.insert(
            "p".into(),
            TableConfig {
                table_type: TableEnum::GlobalKeyValue.into(),
                config: GlobalKeyedTableConfig {
                    table_name: "p".into(),
                    description: "pre-commit data".into(),
                    uses_two_phase_commit: true,
                }
                .encode_to_vec(),
                state_version: 1,
            },
        );
        tables.insert(
            "d".into(),
            TableConfig {
                table_type: TableEnum::GlobalKeyValue.into(),
                config: GlobalKeyedTableConfig {
                    table_name: "d".into(),
                    description: "delta batch data".into(),
                    uses_two_phase_commit: true,
                }
                .encode_to_vec(),
                state_version: 1,
            },
        );
        tables
    }

    fn is_committing(&self) -> bool {
        true
    }

    fn tick_interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    async fn on_start(&mut self, ctx: &mut OperatorContext) -> DataflowResult<()> {
        let schema = ctx.in_schemas.first().unwrap().clone();
        self.event_logger.task_info = Some(ctx.task_info.clone());
        self.context =
            Some(SinkContext::new(&mut self.config, schema, ctx.task_info.clone()).await?);

        let state: HashMap<usize, FilesCheckpointV2> = ctx
            .table_manager
            .get_global_keyed_state_migratable("r")
            .await?
            .take();

        for s in state.values() {
            self.active.max_file_index = self.active.max_file_index.max(s.file_index);
        }

        if ctx.task_info.task_index == 0 && !self.is_delta_write_mode() {
            for s in state.into_values() {
                for f in s.open_files {
                    debug!(path = f.path, buffered_size = f.data.0.len(),
                        state = ?f.state, "recovering and finishing open file");

                    let mut open_file = OpenFile::from_checkpoint(
                        f,
                        self.context.as_ref().unwrap().storage_provider.clone(),
                        &self.config,
                        self.event_logger.clone(),
                    )?;
                    self.upload
                        .pending_uploads
                        .lock()
                        .await
                        .extend(open_file.close()?);
                    self.active
                        .open_files
                        .insert(open_file.path.clone(), open_file);
                }
            }

            let pre_commit_state: &mut GlobalKeyedView<String, FileToCommit> = ctx
                .table_manager
                .get_global_keyed_state_migratable("p")
                .await
                .expect("should be able to get table");

            for f in pre_commit_state.take().into_values() {
                let open_file = OpenFile::from_commit(
                    f,
                    self.context.as_ref().unwrap().storage_provider.clone(),
                    &self.config,
                    self.event_logger.clone(),
                )?;
                self.active
                    .open_files
                    .insert(open_file.path.clone(), open_file);
            }
        }

        maybe_cause_failure("after_start");

        Ok(())
    }

    async fn process_batch(
        &mut self,
        batch: RecordBatch,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let timestamp_index = self.context.as_ref().unwrap().schema.timestamp_index;
        let table_column_index = self.context.as_ref().unwrap().table_column_index;

        // Delta WriteBuilder mode: buffer RecordBatches in memory
        if self.is_delta_write_mode() {
            // Strip _timestamp column
            let col_indices: Vec<usize> = (0..batch.num_columns())
                .filter(|&i| i != timestamp_index)
                .collect();
            let stripped = batch.project(&col_indices).map_err(|e| {
                connector_err!(
                    Internal,
                    NoRetry,
                    source: e.into(),
                    "failed to strip timestamp column"
                )
            })?;

            if let Some(table_col_idx) = table_column_index {
                // DeltaLakeMulti: split by table_column, then strip table_column too
                // Adjust index for the removed timestamp column
                let adjusted_idx = if timestamp_index < table_col_idx {
                    table_col_idx - 1
                } else {
                    table_col_idx
                };

                let table_col = stripped.column(adjusted_idx).as_string::<i32>();

                // Group rows by table name
                let mut table_groups: HashMap<Option<String>, Vec<usize>> = HashMap::new();
                for i in 0..stripped.num_rows() {
                    let table_name = if table_col.is_null(i) {
                        None
                    } else {
                        Some(table_col.value(i).to_string())
                    };
                    table_groups.entry(table_name).or_default().push(i);
                }

                // Column indices excluding the table_column
                let data_col_indices: Vec<usize> = (0..stripped.num_columns())
                    .filter(|&i| i != adjusted_idx)
                    .collect();

                for (table_name, indices) in table_groups {
                    let table_name = match table_name {
                        Some(name) => name,
                        None => {
                            warn!(
                                "Skipping {} rows with null table_column value",
                                indices.len()
                            );
                            continue;
                        }
                    };

                    let indices_array = arrow::array::UInt32Array::from_iter_values(
                        indices.iter().map(|&i| i as u32),
                    );
                    let sub_batch =
                        arrow::compute::take_record_batch(&stripped, &indices_array).map_err(
                            |e| {
                                connector_err!(
                                    Internal,
                                    NoRetry,
                                    source: e.into(),
                                    "failed to take sub-batch for table '{}'",
                                    table_name
                                )
                            },
                        )?;

                    // Strip table_column from the sub_batch
                    let data_batch = sub_batch.project(&data_col_indices).map_err(|e| {
                        connector_err!(
                            Internal,
                            NoRetry,
                            source: e.into(),
                            "failed to strip table_column"
                        )
                    })?;

                    self.active
                        .delta_batch_buffer
                        .push((Some(table_name), data_batch));
                }
            } else {
                // DeltaLake single table
                self.active.delta_batch_buffer.push((None, stripped));
            }

            return Ok(());
        }

        // Non-Delta mode: existing logic
        // If table_column is configured, split by table name first
        if let Some(table_col_idx) = table_column_index {
            let table_col = batch.column(table_col_idx).as_string::<i32>();

            // Group rows by table name
            let mut table_groups: HashMap<Option<String>, Vec<usize>> = HashMap::new();
            for i in 0..batch.num_rows() {
                let table_name = if table_col.is_null(i) {
                    None
                } else {
                    Some(table_col.value(i).to_string())
                };
                table_groups.entry(table_name).or_default().push(i);
            }

            for (table_name, indices) in table_groups {
                let table_name = match table_name {
                    Some(name) => name,
                    None => {
                        warn!(
                            "Skipping {} rows with null table_column value",
                            indices.len()
                        );
                        continue;
                    }
                };

                let indices_array =
                    arrow::array::UInt32Array::from_iter_values(indices.iter().map(|&i| i as u32));
                let sub_batch =
                    arrow::compute::take_record_batch(&batch, &indices_array).map_err(|e| {
                        connector_err!(
                            Internal,
                            NoRetry,
                            source: e.into(),
                            "failed to take sub-batch for table '{}'",
                            table_name
                        )
                    })?;

                self.process_batch_inner(sub_batch, timestamp_index, Some(&table_name))
                    .await?;
            }
        } else {
            self.process_batch_inner(batch, timestamp_index, None)
                .await?;
        }

        Ok(())
    }

    fn future_to_poll(
        &mut self,
    ) -> Option<Pin<Box<dyn Future<Output = Box<dyn Any + Send>> + Send>>> {
        let futures = self.upload.pending_uploads.clone();

        Some(Box::pin(async move {
            let mut guard = futures.lock().await;

            if guard.is_empty() {
                // No pending uploads - wait indefinitely
                // Will be canceled when checkpoint/new batch arrives
                drop(guard);
                futures::future::pending::<()>().await;
                unreachable!()
            }

            // Poll the next upload to completion
            let result: Option<DataflowResult<FsResponse>> = guard.next().await;
            Box::new(result) as Box<dyn Any + Send>
        }))
    }

    async fn handle_future_result(
        &mut self,
        result: Box<dyn Any + Send>,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let result: Option<DataflowResult<FsResponse>> = *result
            .downcast()
            .map_err(|_| connector_err!(Internal, NoRetry, "failed to downcast future result"))?;

        match result {
            None => Ok(()), // FuturesUnordered was empty
            Some(Err(e)) => Err(e),
            Some(Ok(upload_result)) => self.process_upload_result(upload_result).await,
        }
    }

    async fn handle_watermark(
        &mut self,
        watermark: Watermark,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<Option<Watermark>> {
        if let Watermark::EventTime(ts) = watermark {
            self.watermark = Some(ts);
        }

        Ok(Some(watermark))
    }

    async fn handle_checkpoint(
        &mut self,
        barrier: CheckpointBarrier,
        ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        if self.is_delta_write_mode() {
            // Delta WriteBuilder mode: serialize buffered batches via Arrow IPC
            maybe_cause_failure("checkpoint_start");

            let commit_entries: Vec<DeltaBatchCommitData> = self
                .active
                .delta_batch_buffer
                .drain(..)
                .map(|(table_name, batch)| DeltaBatchCommitData {
                    table_name,
                    ipc_data: encode_batch_to_ipc(&batch),
                })
                .collect();

            let serialized =
                bincode::encode_to_vec(&commit_entries, bincode_config::standard()).unwrap();
            ctx.table_manager
                .insert_committing_data("d", serialized)
                .await;

            // Save minimal recovery state (no open files in Delta mode)
            let checkpoint = FilesCheckpointV2 {
                open_files: vec![],
                file_index: self.active.max_file_index,
                delta_version: self.delta_version(),
            };
            let recovery_state: &mut GlobalKeyedView<usize, FilesCheckpointV2> = ctx
                .table_manager
                .get_global_keyed_state("r")
                .await
                .expect("should be able to get table");
            recovery_state
                .insert(ctx.task_info.task_index as usize, checkpoint)
                .await;

            // Still need to insert empty "p" data for non-Delta commit path compatibility
            ctx.table_manager
                .insert_committing_data("p", bincode::encode_to_vec(Vec::<FileToCommit>::new(), bincode_config::standard()).unwrap())
                .await;

            maybe_cause_failure("after_checkpoint");
            return Ok(());
        }

        // Non-Delta mode: existing logic
        // if stopping, close all open files
        if barrier.then_stop {
            let pending = self.upload.pending_uploads.lock().await;
            for f in self.active.open_files.values_mut() {
                for fut in f.close()? {
                    pending.push(fut);
                }
            }
        }

        let commit_strategy = self.commit_strategy();

        maybe_cause_failure("checkpoint_start");

        // then we wait for all pending uploads to finish
        self.flush_pending_uploads().await?;

        maybe_cause_failure("after_flush");

        let mut open_files = vec![];
        let mut files_ready_to_commit = vec![];

        for (file_path, file) in &mut self.active.open_files {
            if file.ready_to_finalize() {
                files_ready_to_commit.push(file.path.clone());
            } else {
                let chk = file.as_checkpoint()?;
                debug!(
                    path = ?file_path,
                    state = ?chk.state,
                    "checkpointing in-progress file",
                );
                open_files.push(chk);
            }
        }

        maybe_cause_failure("after_finalize");

        let mut files_to_commit = vec![];
        mem::swap(&mut self.upload.files_to_commit, &mut files_to_commit);

        let mut upload_futures = FuturesUnordered::new();

        for path in files_ready_to_commit {
            let file = self.active.open_files.remove(&path).unwrap();
            match file.prepare_for_commit(commit_strategy)? {
                CommitPreparation::Serializable(ftc) => {
                    files_to_commit.push(ftc);
                }
                CommitPreparation::UploadThenSerialize(fut) => {
                    upload_futures.push(fut);
                }
                CommitPreparation::LocalCommit(pending) => {
                    open_files.push(pending.as_checkpoint());
                    self.active.pending_local_commits.push(pending);
                }
            }
        }

        // Wait for PerOperator single file uploads to complete
        while let Some(result) = upload_futures.next().await {
            files_to_commit.push(result?);
        }

        let checkpoint = FilesCheckpointV2 {
            open_files,
            file_index: self.active.max_file_index,
            delta_version: self.delta_version(),
        };

        // Save recovery state
        let recovery_state: &mut GlobalKeyedView<usize, FilesCheckpointV2> = ctx
            .table_manager
            .get_global_keyed_state("r")
            .await
            .expect("should be able to get table");
        recovery_state
            .insert(ctx.task_info.task_index as usize, checkpoint)
            .await;

        // Handle pre-commit data
        let serialized =
            bincode::encode_to_vec(&files_to_commit, bincode_config::standard()).unwrap();
        ctx.table_manager
            .insert_committing_data("p", serialized)
            .await;

        maybe_cause_failure("after_checkpoint");
        Ok(())
    }

    async fn handle_commit(
        &mut self,
        epoch: u32,
        commit_data: &HashMap<String, HashMap<u32, Vec<u8>>>,
        ctx: &mut OperatorContext,
    ) -> DataflowResult<()> {
        maybe_cause_failure("commit_start");

        if self.is_delta_write_mode() && ctx.task_info.task_index == 0 {
            // Delta WriteBuilder mode: deserialize batches from all subtasks and write via WriteBuilder
            let all_decoded: Vec<(Option<String>, RecordBatch)> = commit_data
                .get("d")
                .ok_or_else(|| {
                    DataflowError::StateError(StateError::NoRegisteredTable {
                        table: "d".to_string(),
                    })
                })?
                .values()
                .flat_map(|serialized| {
                    let entries: Vec<DeltaBatchCommitData> =
                        bincode::decode_from_slice(serialized, bincode_config::standard())
                            .unwrap()
                            .0;
                    entries
                        .into_iter()
                        .map(|e| (e.table_name, decode_batch_from_ipc(&e.ipc_data)))
                })
                .collect();

            if !all_decoded.is_empty() {
                let partition_cols = self.context.as_ref().unwrap().partition_fields.clone();

                match &mut self.context.as_mut().unwrap().commit_state {
                    CommitState::DeltaLake {
                        last_version,
                        table,
                    } => {
                        let batches: Vec<RecordBatch> =
                            all_decoded.into_iter().map(|(_, b)| b).collect();

                        info!(
                            "Writing {} batches to Delta table via WriteBuilder",
                            batches.len()
                        );

                        let new_version = write_batches_to_delta(
                            table,
                            batches,
                            partition_cols,
                        )
                        .await
                        .map_err(|e| {
                            connector_err!(
                                External,
                                WithBackoff,
                                source: e,
                                "failed to write batches to delta"
                            )
                        })?;
                        *last_version = new_version;
                    }
                    CommitState::DeltaLakeMulti {
                        tables,
                        base_path,
                        storage_options,
                        schema,
                    } => {
                        // Group by table name
                        let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                        for (name, batch) in all_decoded {
                            table_batches
                                .entry(name.unwrap_or_else(|| "unknown".to_string()))
                                .or_default()
                                .push(batch);
                        }

                        let mut commit_count = 0;
                        for (table_name, batches) in table_batches {
                            // Get or create the DeltaTable entry for this table
                            if !tables.contains_key(&table_name) {
                                let table_url = format!("{base_path}/{table_name}");
                                let table_provider =
                                    StorageProvider::for_url_with_options(
                                        &table_url,
                                        storage_options.clone(),
                                    )
                                    .await
                                    .map_err(map_storage_error)?;
                                let delta_table =
                                    load_or_create_table(&table_provider, schema, partition_cols.clone())
                                        .await
                                        .map_err(|e| {
                                            connector_err!(
                                                External,
                                                WithBackoff,
                                                source: e,
                                                "failed to load or create delta table '{}'",
                                                table_name
                                            )
                                        })?;
                                tables.insert(
                                    table_name.clone(),
                                    DeltaTableEntry {
                                        table_path: table_name.clone(),
                                        last_version: -1,
                                        table: Box::new(delta_table),
                                        files: vec![],
                                    },
                                );
                            }

                            let entry = tables.get_mut(&table_name).unwrap();

                            info!(
                                "Writing {} batches to Delta table '{}' via WriteBuilder",
                                batches.len(),
                                table_name
                            );

                            let new_version = write_batches_to_delta(
                                &mut entry.table,
                                batches,
                                partition_cols.clone(),
                            )
                            .await
                            .map_err(|e| {
                                connector_err!(
                                    External,
                                    WithBackoff,
                                    source: e,
                                    "failed to write batches to delta table '{}'",
                                    table_name
                                )
                            })?;
                            entry.last_version = new_version;
                            commit_count += 1;
                        }
                        info!("Committed to {} Delta tables via WriteBuilder", commit_count);
                    }
                    _ => unreachable!("is_delta_write_mode() returned true but commit_state is not Delta"),
                }
            }
        } else if !self.is_delta_write_mode() {
            // Non-Delta mode: existing logic

            // upload single files in PerSubtask mode
            let mut uploads = FuturesUnordered::new();
            for pending in self.active.pending_local_commits.drain(..) {
                uploads.push(pending.finalize());
            }

            while let Some(result) = uploads.next().await {
                let _ = result?;
            }

            if ctx.task_info.task_index == 0 {
                let files_to_commit: Vec<_> = commit_data
                    .get("p")
                    .ok_or_else(|| {
                        DataflowError::StateError(StateError::NoRegisteredTable {
                            table: "p".to_string(),
                        })
                    })?
                    .values()
                    .flat_map(|serialized| {
                        let v: Vec<FileToCommit> =
                            bincode::decode_from_slice(serialized, bincode_config::standard())
                                .unwrap()
                                .0;
                        v
                    })
                    .collect();

                // Separate single files (already uploaded) from multipart files (need finalization)
                let mut finished_files = vec![];
                let mut multipart_files = vec![];

                for file in files_to_commit {
                    debug!(path = ?file.path, data = ?file.typ, "Processing file for commit");

                    match file.typ {
                        checkpoint::FileToCommitType::Single { total_size } => {
                            // Single files are already uploaded during checkpoint,
                            // just add them directly to finished_files
                            finished_files.push(FinishedFile {
                                filename: file.path,
                                // unused, retained for backwards compatibility
                                partition: None,
                                size: total_size,
                                metadata: file.iceberg_metadata,
                            });
                        }
                        checkpoint::FileToCommitType::Multipart { .. } => {
                            multipart_files.push(file);
                        }
                    }
                }

                // Finalize multipart uploads
                let mut futures = FuturesUnordered::new();
                let mut files = HashMap::new();
                for file in multipart_files {
                    let mut file: OpenFile<BBW> = OpenFile::from_commit(
                        file,
                        self.context.as_ref().unwrap().storage_provider.clone(),
                        &self.config,
                        self.event_logger.clone(),
                    )?;
                    futures.push(file.finalize()?);
                    files.insert(file.path.clone(), file);
                }

                // wait for multipart files to be finalized
                while let Some(event) = futures.next().await {
                    let event = event?;
                    let file = files.get_mut(&event.path).ok_or_else(|| {
                        connector_err!(
                            Internal,
                            WithBackoff,
                            "received file event for unknown file during committing: {}",
                            event.path
                        )
                    })?;
                    futures.extend(file.handle_event(event.data)?);
                }

                maybe_cause_failure("commit_middle");

                // Add finalized multipart files to finished_files
                for f in files.into_values() {
                    let filename = f.path.to_string();
                    let (total_size, metadata) = f.metadata_for_closed()?;

                    finished_files.push(FinishedFile {
                        filename,
                        partition: None,
                        size: total_size,
                        metadata,
                    })
                }

                // finally, commit them if we're using a table format
                match &mut self.context.as_mut().unwrap().commit_state {
                    CommitState::Iceberg(table) => {
                        table.commit(epoch, &finished_files).await.map_err(
                            |e| connector_err!(External, WithBackoff, source: e, "failed to commit to iceberg"),
                        )?;
                    }
                    CommitState::VanillaParquet => {
                        // Nothing to do
                    }
                    _ => {
                        // DeltaLake/DeltaLakeMulti handled above in delta write mode
                    }
                }
            }
        }

        maybe_cause_failure("commit_before_ack");

        // Send completion event
        ctx.control_tx
            .send(arroyo_rpc::ControlResp::CheckpointEvent(CheckpointEvent {
                checkpoint_epoch: epoch,
                node_id: ctx.task_info.node_id,
                operator_id: ctx.task_info.operator_id.clone(),
                subtask_index: ctx.task_info.task_index,
                time: SystemTime::now(),
                event_type: arroyo_rpc::grpc::rpc::TaskCheckpointEventType::FinishedCommit,
            }))
            .await
            .expect("sent commit event");

        maybe_cause_failure("commit_after_ack");

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        _tick: u64,
        _ctx: &mut OperatorContext,
        _collector: &mut dyn Collector,
    ) -> DataflowResult<()> {
        let mut to_remove = vec![];
        for (partition, path) in self.active.active_partitions.iter() {
            let Some(of) = self.active.open_files.get_mut(path) else {
                warn!(file = ?path, "file referenced in active_partitions is missing from open_files!");
                to_remove.push(partition.clone());
                continue;
            };

            if self
                .upload
                .roll_file_if_ready(&self.config.rolling_policies, self.watermark, of)
                .await?
            {
                to_remove.push(partition.clone());
            }
        }

        for p in to_remove {
            self.active.active_partitions.remove(&p);
        }

        Ok(())
    }
}
