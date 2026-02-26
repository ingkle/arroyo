mod checkpoint;
pub mod migration;
mod open_file;
mod uploads;

use super::delta::{commit_files_to_delta, load_or_create_table};
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
use arrow::array::{Array, StringArray, UInt32Array};
use arrow::compute::take;
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
use bincode::config as bincode_config;
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

/// (table_prefix, hive_partition) — table_prefix is Some only in multi-table mode
pub type TablePartitionKey = (Option<String>, Option<OwnedRow>);

pub struct ActiveState<BBW: BatchBufferingWriter + 'static> {
    max_file_index: usize,
    // (table_prefix, partition) -> filename
    active_partitions: HashMap<TablePartitionKey, Arc<Path>>,
    // filename -> file writer
    open_files: HashMap<Arc<Path>, OpenFile<BBW>>,
    // single files pending local commit (PerSubtask strategy)
    pending_local_commits: Vec<PendingSingleFile>,
}

pub struct SinkConfig {
    config: config::FileSystemSink,
    format: Format,
    file_naming: NamingConfig,
    partitioner_mode: PartitionerMode,
    rolling_policies: Vec<RollingPolicy>,
    // consumed on start
    table_format: Option<TableFormat>,
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
        table_column: Option<&str>,
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

        let commit_state = match table_format {
            TableFormat::Delta => {
                if table_column.is_some() {
                    CommitState::DeltaLakeMulti {
                        tables: HashMap::new(),
                        base_path: config.config.path.clone(),
                        storage_options: config.config.storage_options.clone(),
                        schema: schema.schema_without_timestamp(),
                    }
                } else {
                    CommitState::DeltaLake {
                        last_version: -1,
                        table: Box::new(
                            load_or_create_table(&provider, &schema.schema_without_timestamp())
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
                    }
                }
            }
            TableFormat::Iceberg(mut table) => {
                let t = table
                    .load_or_create(task_info.clone(), &schema.schema)
                    .await?;
                iceberg_schema = Some(t.metadata().current_schema().clone());
                CommitState::Iceberg(table)
            }
            TableFormat::None => CommitState::VanillaParquet,
        };

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

        if let Some(prefix) = table_prefix {
            filename = format!("{prefix}/{filename}");
        }

        filename
    }

    fn get_or_create_file(
        &mut self,
        config: &SinkConfig,
        logger: FsEventLogger,
        context: &SinkContext,
        key: &TablePartitionKey,
        representative_ts: SystemTime,
    ) -> &mut OpenFile<BBW> {
        let file = self
            .active_partitions
            .get(key)
            .and_then(|f| self.open_files.get(f));

        if file.is_none() || !file.unwrap().is_writable() {
            let file_path = self.next_file_path(config, context, &key.1, key.0.as_deref());
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
                .insert(key.clone(), path.clone());
            self.open_files.insert(path, open_file);

            self.max_file_index += 1;
        }

        let file_path = self.active_partitions.get(key).unwrap();
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
    table_column: Option<String>,
    table_column_index: Option<usize>,
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
            file_naming.suffix = Some(BBW::suffix());
        }

        let connection_id_str = connection_id.clone().unwrap_or_default();

        Self {
            config: SinkConfig {
                rolling_policies: RollingPolicy::from_file_settings(&config),
                config,
                format,
                table_format: Some(table_format),
                file_naming,
                partitioner_mode,
            },
            context: None,
            active: ActiveState {
                active_partitions: HashMap::new(),
                open_files: HashMap::new(),
                max_file_index: 0,
                pending_local_commits: vec![],
            },
            upload: UploadState {
                pending_uploads: Arc::new(Mutex::new(FuturesUnordered::new())),
                files_to_commit: Vec::new(),
            },
            event_logger: FsEventLogger {
                task_info: None,
                connection_id: connection_id_str.into(),
            },
            watermark: None,
            table_column,
            table_column_index: None,
        }
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
            _ => -1,
        }
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

        // Resolve table_column index from schema
        if let Some(ref col_name) = self.table_column {
            match schema.schema.index_of(col_name) {
                Ok(idx) => {
                    self.table_column_index = Some(idx);
                    info!(
                        "Delta sink: dynamic table routing from column '{}' (index {})",
                        col_name, idx
                    );
                }
                Err(_) => {
                    warn!(
                        "Delta sink: table_column '{}' not found in schema, using single table mode",
                        col_name
                    );
                }
            }
        }

        self.context = Some(
            SinkContext::new(
                &mut self.config,
                schema,
                ctx.task_info.clone(),
                self.table_column.as_deref(),
            )
            .await?,
        );

        let state: HashMap<usize, FilesCheckpointV2> = ctx
            .table_manager
            .get_global_keyed_state_migratable("r")
            .await?
            .take();

        for s in state.values() {
            self.active.max_file_index = self.active.max_file_index.max(s.file_index);
        }

        if ctx.task_info.task_index == 0 {
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
        let partitioner = &self.context.as_ref().unwrap().partitioner;

        if let Some(col_idx) = self.table_column_index {
            // Multi-table mode: split batch by table column value first
            let groups = group_batch_by_string_column(&batch, col_idx)?;

            for (table_value, sub_batch) in groups {
                let partitions: Vec<(TablePartitionKey, RecordBatch)> =
                    if partitioner.is_partitioned() {
                        partitioner
                            .partition(&sub_batch)?
                            .into_iter()
                            .map(|(k, b)| ((Some(table_value.clone()), Some(k)), b))
                            .collect()
                    } else {
                        vec![((Some(table_value), None), sub_batch)]
                    };

                for (key, part_batch) in partitions {
                    let representative_timestamp =
                        representitive_timestamp(part_batch.column(timestamp_index)).map_err(
                            |e| {
                                connector_err!(
                                    Internal,
                                    NoRetry,
                                    source: e,
                                    "failed to get representative timestamp"
                                )
                            },
                        )?;

                    let file = self.active.get_or_create_file(
                        &self.config,
                        self.event_logger.clone(),
                        self.context.as_ref().unwrap(),
                        &key,
                        representative_timestamp,
                    );
                    let future = file.add_batch(&part_batch)?;

                    if let Some(future) = future {
                        let futures = self.upload.pending_uploads.lock().await;
                        futures.push(future);
                    }

                    if self
                        .upload
                        .roll_file_if_ready(&self.config.rolling_policies, self.watermark, file)
                        .await?
                    {
                        self.active.active_partitions.remove(&key);
                    }
                }
            }
        } else {
            // Single-table mode
            let partitions: Vec<(TablePartitionKey, RecordBatch)> =
                if partitioner.is_partitioned() {
                    partitioner
                        .partition(&batch)?
                        .into_iter()
                        .map(|(k, b)| ((None, Some(k)), b))
                        .collect()
                } else {
                    vec![((None, None), batch)]
                };

            for (key, sub_batch) in partitions {
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
                    &key,
                    representative_timestamp,
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
                    self.active.active_partitions.remove(&key);
                }
            }
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
                CommitState::DeltaLake {
                    last_version,
                    table,
                } => {
                    if let Some(new_version) = commit_files_to_delta(
                        &finished_files,
                        table,
                        *last_version,
                    )
                        .await
                        .map_err(
                            |e| connector_err!(External, WithBackoff, source: e, "failed to commit to delta"),
                        )? {
                        *last_version = new_version;
                    }
                }
                CommitState::DeltaLakeMulti {
                    tables,
                    base_path,
                    storage_options,
                    schema,
                } => {
                    // Group finished files by table prefix
                    let mut table_files: HashMap<String, Vec<FinishedFile>> = HashMap::new();
                    for file in &finished_files {
                        let (table_name, relative_path) = split_table_prefix(&file.filename);
                        table_files
                            .entry(table_name)
                            .or_default()
                            .push(FinishedFile {
                                filename: relative_path,
                                partition: file.partition.clone(),
                                size: file.size,
                                metadata: file.metadata.clone(),
                            });
                    }

                    // Commit to each table independently
                    for (table_name, files) in table_files {
                        if !tables.contains_key(&table_name) {
                            // Lazy create DeltaTable for this sub-path
                            let sub_path = format!("{}/{}", base_path, table_name);
                            let provider = StorageProvider::for_url_with_options(
                                &sub_path,
                                storage_options.clone(),
                            )
                            .await
                            .map_err(|e| {
                                connector_err!(
                                    User,
                                    NoRetry,
                                    "failed to create storage provider for table '{}': {:?}",
                                    table_name,
                                    e
                                )
                            })?;
                            let dt = load_or_create_table(&provider, schema)
                                .await
                                .map_err(|e| {
                                    connector_err!(
                                        User,
                                        NoRetry,
                                        source: e,
                                        "failed to load or create delta table for '{}'",
                                        table_name
                                    )
                                })?;
                            let version = dt.version().unwrap_or(-1);
                            tables.insert(
                                table_name.clone(),
                                DeltaTableEntry {
                                    table: dt,
                                    last_version: version,
                                },
                            );
                        }

                        let entry = tables.get_mut(&table_name).unwrap();
                        if let Some(new_version) = commit_files_to_delta(
                            &files,
                            &mut entry.table,
                            entry.last_version,
                        )
                        .await
                        .map_err(|e| {
                            connector_err!(
                                External,
                                WithBackoff,
                                source: e,
                                "failed to commit to delta table '{}'",
                                table_name
                            )
                        })? {
                            entry.last_version = new_version;
                        }
                    }
                }
                CommitState::Iceberg(table) => {
                    table.commit(epoch, &finished_files).await.map_err(
                        |e| connector_err!(External, WithBackoff, source: e, "failed to commit to iceberg"),
                    )?;
                }
                CommitState::VanillaParquet => {
                    // Nothing to do
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
        for (key, path) in self.active.active_partitions.iter() {
            let Some(of) = self.active.open_files.get_mut(path) else {
                warn!(file = ?path, "file referenced in active_partitions is missing from open_files!");
                to_remove.push(key.clone());
                continue;
            };

            if self
                .upload
                .roll_file_if_ready(&self.config.rolling_policies, self.watermark, of)
                .await?
            {
                to_remove.push(key.clone());
            }
        }

        for p in to_remove {
            self.active.active_partitions.remove(&p);
        }

        Ok(())
    }
}

fn group_batch_by_string_column(
    batch: &RecordBatch,
    col_idx: usize,
) -> DataflowResult<HashMap<String, RecordBatch>> {
    let col = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            connector_err!(
                User,
                NoRetry,
                "table_column must be a string column"
            )
        })?;

    let mut indices: HashMap<String, Vec<u32>> = HashMap::new();
    for i in 0..col.len() {
        if col.is_valid(i) {
            let val = col.value(i).to_string();
            indices.entry(val).or_default().push(i as u32);
        }
    }

    let schema = batch.schema();
    indices
        .into_iter()
        .map(|(key, idx)| {
            let idx_arr = UInt32Array::from(idx);
            let columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|c| take(c, &idx_arr, None))
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    connector_err!(
                        Internal,
                        NoRetry,
                        "failed to split batch by table column: {e}"
                    )
                })?;
            Ok((
                key,
                RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                    connector_err!(Internal, NoRetry, "failed to create sub-batch: {e}")
                })?,
            ))
        })
        .collect()
}

fn split_table_prefix(filename: &str) -> (String, String) {
    let clean = filename.trim_start_matches('/');
    if let Some(pos) = clean.find('/') {
        (clean[..pos].to_string(), clean[pos + 1..].to_string())
    } else {
        (clean.to_string(), String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_split_table_prefix_with_partition() {
        let (table, path) = split_table_prefix("edge-001/event_date=2026-02-25/00001.parquet");
        assert_eq!(table, "edge-001");
        assert_eq!(path, "event_date=2026-02-25/00001.parquet");
    }

    #[test]
    fn test_split_table_prefix_no_subpath() {
        let (table, path) = split_table_prefix("edge-001");
        assert_eq!(table, "edge-001");
        assert_eq!(path, "");
    }

    #[test]
    fn test_split_table_prefix_leading_slash() {
        let (table, path) = split_table_prefix("/edge-002/data.parquet");
        assert_eq!(table, "edge-002");
        assert_eq!(path, "data.parquet");
    }

    #[test]
    fn test_split_table_prefix_nested_partitions() {
        let (table, path) =
            split_table_prefix("edge-003/year=2026/month=02/00001-000.parquet");
        assert_eq!(table, "edge-003");
        assert_eq!(path, "year=2026/month=02/00001-000.parquet");
    }

    #[test]
    fn test_group_batch_by_string_column_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        let groups = group_batch_by_string_column(&batch, 0).unwrap();

        assert_eq!(groups.len(), 2);

        let group_a = &groups["a"];
        assert_eq!(group_a.num_rows(), 3);
        let vals_a: Vec<i64> = group_a
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(vals_a, vec![1, 3, 5]);

        let group_b = &groups["b"];
        assert_eq!(group_b.num_rows(), 2);
        let vals_b: Vec<i64> = group_b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(vals_b, vec![2, 4]);
    }

    #[test]
    fn test_group_batch_by_string_column_single_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["x", "x", "x"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let groups = group_batch_by_string_column(&batch, 0).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups["x"].num_rows(), 3);
    }

    #[test]
    fn test_group_batch_skips_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("a"), None])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let groups = group_batch_by_string_column(&batch, 0).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups["a"].num_rows(), 2);
    }

    #[test]
    fn test_group_batch_many_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, false),
            Field::new("sensor", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "edge-001", "edge-002", "edge-003", "edge-001", "edge-002", "edge-003",
                ])),
                Arc::new(StringArray::from(vec![
                    "temp", "temp", "humidity", "humidity", "temp", "humidity",
                ])),
                Arc::new(Int64Array::from(vec![25, 30, 60, 55, 28, 65])),
            ],
        )
        .unwrap();

        let groups = group_batch_by_string_column(&batch, 0).unwrap();
        assert_eq!(groups.len(), 3);
        assert_eq!(groups["edge-001"].num_rows(), 2);
        assert_eq!(groups["edge-002"].num_rows(), 2);
        assert_eq!(groups["edge-003"].num_rows(), 2);

        // Verify column integrity after split
        let g1_sensors: Vec<&str> = groups["edge-001"]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(g1_sensors, vec!["temp", "humidity"]);
    }

    #[test]
    fn test_table_partition_key_as_hashmap_key() {
        let mut map: HashMap<TablePartitionKey, String> = HashMap::new();
        map.insert((Some("edge-001".to_string()), None), "file1".to_string());
        map.insert((Some("edge-002".to_string()), None), "file2".to_string());
        map.insert((None, None), "file3".to_string());

        assert_eq!(map.len(), 3);
        assert_eq!(
            map.get(&(Some("edge-001".to_string()), None)),
            Some(&"file1".to_string())
        );
        assert_eq!(
            map.get(&(None, None)),
            Some(&"file3".to_string())
        );
    }

    /// E2E: write parquet files to local temp dirs, commit via Delta, read back
    #[tokio::test]
    async fn test_delta_multi_table_commit_e2e() {
        use super::super::delta::{commit_files_to_delta, load_or_create_table};
        use super::super::{DeltaTableEntry, FinishedFile};
        use arroyo_storage::StorageProvider;
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap as StdHashMap;
        use tempfile::TempDir;

        let base_dir = TempDir::new().unwrap();
        let base_path = base_dir.path().to_str().unwrap().to_string();

        // Schema: edge_id (string), value (int64)
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        // Simulate: process_batch produced files under table prefixes
        let edge_ids = vec!["edge-001", "edge-002"];
        let mut all_finished: Vec<FinishedFile> = vec![];

        for edge_id in &edge_ids {
            let sub_dir = base_dir.path().join(edge_id);
            std::fs::create_dir_all(&sub_dir).unwrap();

            // Write a real parquet file
            let file_name = "00001-000.parquet";
            let file_path = sub_dir.join(file_name);

            let batch = RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![*edge_id; 3])),
                    Arc::new(Int64Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();

            let file = std::fs::File::create(&file_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let file_size = std::fs::metadata(&file_path).unwrap().len() as usize;

            // FinishedFile with table prefix (as produced by process_batch)
            all_finished.push(FinishedFile {
                filename: format!("{}/{}", edge_id, file_name),
                partition: None,
                size: file_size,
                metadata: None,
            });
        }

        // --- Simulate DeltaLakeMulti commit logic (mirrors handle_commit) ---
        let mut tables: StdHashMap<String, DeltaTableEntry> = StdHashMap::new();

        // Group by table prefix
        let mut table_files: StdHashMap<String, Vec<FinishedFile>> = StdHashMap::new();
        for file in &all_finished {
            let (table_name, relative_path) = split_table_prefix(&file.filename);
            table_files
                .entry(table_name)
                .or_default()
                .push(FinishedFile {
                    filename: relative_path,
                    partition: file.partition.clone(),
                    size: file.size,
                    metadata: file.metadata.clone(),
                });
        }

        assert_eq!(table_files.len(), 2, "should have 2 table groups");

        // Commit each table
        for (table_name, files) in &table_files {
            let sub_path = format!("file://{}/{}", base_path, table_name);
            let provider = StorageProvider::for_url_with_options(&sub_path, HashMap::new())
                .await
                .unwrap();

            let dt = load_or_create_table(&provider, &arrow_schema).await.unwrap();
            let version = dt.version().unwrap_or(-1);
            tables.insert(
                table_name.clone(),
                DeltaTableEntry {
                    table: dt,
                    last_version: version,
                },
            );

            let entry = tables.get_mut(table_name).unwrap();
            let new_version =
                commit_files_to_delta(files, &mut entry.table, entry.last_version)
                    .await
                    .unwrap();
            if let Some(v) = new_version {
                entry.last_version = v;
            }
        }

        // --- Verify ---
        // 1. Each edge has its own _delta_log
        for edge_id in &edge_ids {
            let delta_log = base_dir.path().join(edge_id).join("_delta_log");
            assert!(
                delta_log.exists(),
                "_delta_log should exist for {}",
                edge_id
            );
        }

        // 2. Each table was committed (version >= 0)
        for (name, entry) in &tables {
            assert!(
                entry.last_version >= 0,
                "table {} should have version >= 0, got {}",
                name,
                entry.last_version
            );
        }

        // 3. Reload tables and verify file count
        for edge_id in &edge_ids {
            let sub_path = format!("file://{}/{}", base_path, edge_id);
            let provider = StorageProvider::for_url_with_options(&sub_path, HashMap::new())
                .await
                .unwrap();
            let table = load_or_create_table(&provider, &arrow_schema).await.unwrap();

            let files = table.get_files_iter().unwrap().collect::<Vec<_>>();
            assert_eq!(
                files.len(),
                1,
                "{} should have exactly 1 data file",
                edge_id
            );
        }

        // 4. No _delta_log at base level (only in sub-tables)
        let base_delta_log = base_dir.path().join("_delta_log");
        assert!(
            !base_delta_log.exists(),
            "base path should NOT have _delta_log"
        );
    }
}
