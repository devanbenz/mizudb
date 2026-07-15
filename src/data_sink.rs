use crate::wal::MizuWAL;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::parquet::ParquetSink;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

// TODO: Datasink needs to essentially write to WAL
// on BEGIN; ---- COMMIT; for transactions, we
// also should write to WAL after 5-10ms of write time or inactivity in writes.
// Hold data in memory buffer until that 5-10ms finishes.
// Once we have 10 MB of data in our WAL we commit and flush to disk, after disk
// flush is finished we truncate the WAL file.

pub(crate) struct MizuDataSink {
    buffer: Arc<Mutex<Vec<SendableRecordBatchStream>>>,
    parquet_sink: Arc<ParquetSink>,
    wal: Arc<MizuWAL>,
}

impl MizuDataSink {
    pub(crate) fn new(config: FileSinkConfig, wal: Arc<MizuWAL>) -> Self {
        let parquet_sink = Arc::new(ParquetSink::new(config, TableParquetOptions::default()));
        Self { parquet_sink, wal, buffer: Arc::new(Mutex::new(Vec::new())) }
    }

    pub(crate) fn get_buffer(&self) -> Arc<Mutex<Vec<SendableRecordBatchStream>>> {
        self.buffer.clone()
    }

    pub(crate) fn get_wal(&self) -> Arc<MizuWAL> {
        self.wal.clone()
    }

    pub(crate) fn get_parquet_sink(&self) -> Arc<ParquetSink> {
        self.parquet_sink.clone()
    }

    pub(crate) fn background_sink_job(&self) {}
}

impl DisplayAs for MizuDataSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl Debug for MizuDataSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl DataSink for MizuDataSink {
    fn schema(&self) -> &SchemaRef {
        todo!()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        self.parquet_sink.write_all(data, context).await
    }
}
