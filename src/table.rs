use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{plan_err, SchemaExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode, FileScanConfigBuilder, FileSinkConfig, FileSource};
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::Expr;
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
    options: ListingOptions,
}

impl MizuTable {
    pub fn new(
        schema: SchemaRef,
        object_store_url: ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        table_path: ListingTableUrl,
    ) -> Self {
        let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::new());
        MizuTable {
            schema,
            file_source,
            object_store_url,
            table_paths: vec![table_path],
            options: ListingOptions::new(format),
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
        println!("meta: {:#?}", partitioned_file);
        let file_scan_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), self.file_source.clone())
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_file(partitioned_file)
                .build();

        Ok(self.options().format.create_physical_plan(state, file_scan_config).await?)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        println!("input: {:#?}", input);
        // Check that the schema of the plan matches the schema of this table.
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        let table_path = &self.table_paths()[0];

        // Inverted check: we now require the path to point at exactly one file.
        if table_path.is_collection() {
            return plan_err!(
            "Inserting requires a table backed by a single file, but the URL is a \
             collection (it ends with a trailing `/`). Point the table at one file instead."
        );
        }

        // No partition listing needed for a single file. If the file already
        // exists, include it in the file group so InsertOp::Overwrite knows
        // what it is replacing; otherwise start with an empty group.
        let store = state.runtime_env().object_store(table_path)?;
        let file_group = match store.head(table_path.prefix()).await {
            Ok(meta) => vec![PartitionedFile::from(meta)].into(),
            Err(_) => FileGroup::default(),
        };

        let keep_partition_by_columns =
            state.config_options().execution.keep_partition_by_columns;

        // Sink related options, apart from format
        let config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: table_path.object_store(),
            table_paths: self.table_paths().clone(),
            file_group,
            output_schema: self.schema(),
            // Hive-style partitioning is meaningless for a single output file
            table_partition_cols: vec![],
            insert_op,
            keep_partition_by_columns,
            file_extension: self.options().format.get_ext(),
            // Force all output rows into one file rather than letting the
            // sink demux into multiple files
            file_output_mode: FileOutputMode::SingleFile,
        };

        // For writes, we only use user-specified ordering (no file groups to derive from)
        let orderings = self.try_create_output_ordering(state.execution_props(), &[])?;
        // It is sufficient to pass only one of the equivalent orderings:
        let order_requirements = orderings.into_iter().next().map(Into::into);

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }
}
