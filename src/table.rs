use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{ScanResult, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};

struct MizuTable {
    schema: SchemaRef,
    format: Arc<dyn FileFormat>,
}

impl MizuTable {
    fn new(schema: SchemaRef) -> Self {
        MizuTable {
            schema,
            format: Arc::new(ParquetFormat::new()),
        }
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: Finish scan method
        self.format.create_physical_plan(state, FileScanConfigBuilder::new())
        ScanResult::new(state.create_physical_plan(LogicalPlanBuilder::new())?)
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
