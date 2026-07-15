use crate::catalog::MizuSchemaProvider;
use crate::disk_manager::MizuDiskManager;
use datafusion::object_store::path::Path;
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
    schema_provider: Arc<MizuSchemaProvider>,
    disk_manager: Arc<MizuDiskManager>,
}

impl MizuWAL {
    pub fn new(
        path: Path,
        schema_provider: Arc<MizuSchemaProvider>,
        disk_manager: Arc<MizuDiskManager>,
    ) -> Self {
        Self {
            path,
            schema_provider,
            disk_manager,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_basic_test() {
        let mwal = MizuWAL::new(
            Path::parse("test").unwrap(),
            Arc::new(MizuSchemaProvider::new()),
            Arc::new(MizuDiskManager::new()),
        );
    }
}
