use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::object_store::path::Path;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures_util::StreamExt;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

pub enum WALOperation {
    Create,
    Update,
    Delete,
    Checkpoint,
}

pub struct WALEntry {
    operation: WALOperation,
    record_batches: Arc<RwLock<Vec<datafusion::arrow::record_batch::RecordBatch>>>,
}

impl WALEntry {
    pub fn new(path: Path, operation: WALOperation) -> Self {
        Self {
            operation,
            record_batches: Arc::new(RwLock::new(vec![])),
        }
    }

    pub fn add_record_batch(&self, record_batch: datafusion::arrow::record_batch::RecordBatch) {
        self.record_batches.write().unwrap().push(record_batch);
    }

    pub fn record_batches(&self) -> Arc<RwLock<Vec<datafusion::arrow::record_batch::RecordBatch>>> {
        self.record_batches.clone()
    }

    pub fn operation(&self) -> &WALOperation {
        &self.operation
    }

    pub fn flush(&self) {
        self.record_batches.write().unwrap().clear();
    }
}

pub struct MizuWAL {
    path: Path,
    schema: SchemaRef,
}

impl MizuWAL {
    pub fn new(path: Path, schema: SchemaRef) -> Self {
        Self { path, schema }
    }

    pub(crate) fn exec(&self, entry: WALEntry) {
        match entry.operation() {
            WALOperation::Create => {}
            WALOperation::Update => {}
            WALOperation::Delete => {}
            WALOperation::Checkpoint => {}
        }
    }

    pub(crate) fn read(&self, operation: WALOperation) -> Option<WALEntry> {
        todo!()
    }

    pub(crate) fn truncate(&self) {
        todo!()
    }
}

impl DisplayAs for MizuWAL {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl Debug for MizuWAL {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl DataSink for MizuWAL {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut data = data.fuse();
        while let Some(batch) = data.next().await {
            let batch = batch?;

            todo!()
        }

        Ok(0)
    }
}

#[cfg(test)]
mod tests {

    // #[test]
    // fn wal_basic_test() {
    //     let mwal = MizuWAL::new(
    //         Path::parse("test").unwrap(),
    //         Arc::new(Schema::new(vec![])),
    //     );
    // }
}
