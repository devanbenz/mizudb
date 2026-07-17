use crate::data_sink::MizuDataSink;
use crate::disk_manager::{MizuDiskManager, MizuDiskManagerCacheEntry};
use crate::wal::MizuWAL;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{plan_err, SchemaExt};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::parquet::ParquetSink;
use datafusion::datasource::physical_plan::{
    FileGroup, FileOutputMode, FileScanConfigBuilder, FileSinkConfig, FileSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::object_store::path::Path;
use datafusion::object_store::ObjectStoreExt;
use datafusion::physical_expr::{create_lex_ordering, LexOrdering};
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct MizuTable {
    schema: SchemaRef,
    file_source: Arc<dyn FileSource>,
    object_store_url: ObjectStoreUrl,
    table_paths: Vec<ListingTableUrl>,
    disk_manager: Arc<MizuDiskManager>,
    options: ListingOptions,
}

impl MizuTable {
    pub fn new(
        schema: SchemaRef,
        object_store_url: ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        table_path: ListingTableUrl,
        disk_manager: Arc<MizuDiskManager>,
    ) -> Self {
        let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::new());
        MizuTable {
            schema,
            file_source,
            object_store_url,
            table_paths: vec![table_path],
            options: ListingOptions::new(format),
            disk_manager,
        }
    }

    fn table_paths(&self) -> &Vec<ListingTableUrl> {
        &self.table_paths
    }

    fn options(&self) -> &ListingOptions {
        &self.options
    }

    fn try_create_output_ordering(
        &self,
        execution_props: &ExecutionProps,
        _file_groups: &[FileGroup],
    ) -> datafusion::common::Result<Vec<LexOrdering>> {
        if !self.options.file_sort_order.is_empty() {
            return create_lex_ordering(
                &self.schema,
                &self.options.file_sort_order,
                execution_props,
            );
        }
        Ok(vec![])
    }
}

impl Debug for MizuTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuTable")
    }
}

#[async_trait]
impl TableProvider for MizuTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // TODO: Implement filters
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let store = state.runtime_env().object_store(&self.table_paths()[0])?;
        let meta = store.head(&self.table_paths()[0].prefix()).await?;
        let partitioned_file = PartitionedFile::from(meta);
        let file_scan_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), self.file_source.clone())
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_file(partitioned_file)
                .build();

        Ok(self
            .options()
            .format
            .create_physical_plan(state, file_scan_config)
            .await?)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        let table_path = &self.table_paths()[0];

        if table_path.is_collection() {
            return plan_err!(
                "Inserting requires a table backed by a single file, but the URL is a \
             collection (it ends with a trailing `/`). Point the table at one file instead."
            );
        }

        let store = state.runtime_env().object_store(table_path)?;
        // This is a single file table, so we can just use the head() method to get the metadata.
        let file_group = match store.head(table_path.prefix()).await {
            Ok(meta) => vec![PartitionedFile::from(meta)].into(),
            Err(_) => FileGroup::default(),
        };

        let keep_partition_by_columns = state.config_options().execution.keep_partition_by_columns;

        // TODO: Check if ParquetSink and WAL are in cache, if not create them.
        let config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: table_path.object_store(),
            table_paths: self.table_paths().clone(),
            file_group,
            output_schema: self.schema(),
            table_partition_cols: vec![],
            insert_op,
            keep_partition_by_columns,
            file_extension: self.options().format.get_ext(),
            file_output_mode: FileOutputMode::SingleFile,
        };

        let orderings = self.try_create_output_ordering(state.execution_props(), &[])?;
        let order_requirements = orderings.into_iter().next().map(Into::into);

        if let Some(stream_name) = table_path.prefix().filename() {
            // Let's just write directly to the catalog if we're creating tables for now.
            // TODO: Might need to implement WAL for recovery for catalog too.
            if stream_name.eq("catalog.parquet") {
                return self.options()
                    .format
                    .create_writer_physical_plan(input, state, config, order_requirements)
                    .await;
            }
            let parquet_sink = Arc::new(ParquetSink::new(config, TableParquetOptions::default()));

            let cache_entry = MizuDiskManagerCacheEntry::new(
                Arc::new(MizuWAL::new(Path::parse(table_path.prefix().to_string()).expect("REASON"), self.schema.clone())),
                parquet_sink,
                self.schema(),
                0,
            );
            self.disk_manager.insert_if_not_exists(stream_name, cache_entry);

            let sink = Arc::new(MizuDataSink {
                schema: self.schema(),
                stream_name: stream_name.parse().unwrap(),
                disk_manager: self.disk_manager.clone(),
            });

            Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
        } else {
            plan_err!("Table path must have a filename")
        }
    }
}
