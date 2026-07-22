use crate::disk_manager::{MizuDataSinkJob, MizuDiskManager};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::sync::oneshot;

// TODO: Datasink needs to essentially write to WAL
// on BEGIN; ---- COMMIT; for transactions, we
// also should write to WAL after 5-10ms of write time or inactivity in writes.
// Hold data in memory buffer until that 5-10ms finishes.
// Once we have 10 MB of data in our WAL we commit and flush to disk, after disk
// flush is finished we truncate the WAL file.

pub(crate) struct MizuDataSink {
    pub(crate) schema: SchemaRef,
    pub(crate) stream_name: String,
    pub(crate) disk_manager: Arc<MizuDiskManager>,
}

impl MizuDataSink {
    pub(crate) fn new(
        schema: SchemaRef,
        stream_name: String,
        disk_manager: Arc<MizuDiskManager>,
    ) -> Self {
        Self {
            schema,
            stream_name,
            disk_manager,
        }
    }
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
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let (completion_sender, completion_receiver) =
            oneshot::channel::<datafusion::common::Result<u64>>();
        let schema = data.schema();
        // TODO: Maybe use a &str instead of String
        let stream_name = self.stream_name.clone();
        self.disk_manager
            .send_job(MizuDataSinkJob::BufferWrite {
                record_batch: data,
                context: context.clone(),
                schema,
                stream_name,
                completion: completion_sender,
            })
            .await?;

        match completion_receiver.await {
            Ok(result) => result,
            Err(err) => Err(DataFusionError::Execution(format!(
                "Failed to receive completion: {}",
                err
            ))),
        }
    }
}
