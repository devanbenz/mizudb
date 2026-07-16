use crate::data_sink::MizuDataSinkBuffer;
use crate::wal::MizuWAL;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use futures_util::{FutureExt, StreamExt};
use log::info;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

pub(crate) enum MizuDataSinkJob {
    ParquetWrite {
        record_batch: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    },
    WALWrite {
        record_batch: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    },
    BufferWrite {
        record_batch: SendableRecordBatchStream,
        context: Arc<TaskContext>,
        buffer: Arc<Mutex<Vec<MizuDataSinkBuffer>>>,
        completion: oneshot::Sender<datafusion::common::Result<u64>>,
    },
}

const DEFAULT_NUM_THREADS: usize = 4;

pub(crate) struct MizuDiskManager {
    buffer: Arc<Mutex<Vec<MizuDataSinkBuffer>>>,
    parquet_sink: Arc<ParquetSink>,
    wal: Arc<MizuWAL>,
    job_pool: Arc<Mutex<rayon::ThreadPool>>,
    sender: mpsc::UnboundedSender<MizuDataSinkJob>,
    receiver: Mutex<mpsc::UnboundedReceiver<MizuDataSinkJob>>,
}

impl MizuDiskManager {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<MizuDataSinkJob>();
        let thread_pool = rayon::ThreadPoolBuilder::new().num_threads(DEFAULT_NUM_THREADS).build().expect("Failed to create thread pool");
        let disk_manager = Self { job_pool: Arc::new(Mutex::new(thread_pool)), sender: tx, receiver: Mutex::new(rx) };
        disk_manager.start_background_sink_job().now_or_never();
        disk_manager
    }

    pub(crate) fn get_buffer(&self) -> Arc<Mutex<Vec<MizuDataSinkBuffer>>> {
        self.buffer.clone()
    }

    pub(crate) fn get_wal(&self) -> Arc<MizuWAL> {
        self.wal.clone()
    }

    pub(crate) fn get_parquet_sink(&self) -> Arc<ParquetSink> {
        self.parquet_sink.clone()
    }

    async fn parquet_write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        self.parquet_sink.write_all(data, context).await
    }

    pub(crate) fn send_job(&self, job: MizuDataSinkJob) -> datafusion::common::Result<()> {
        match self.sender.clone().send(job) {
            Ok(_) => Ok(()),
            Err(err) => {
                Err(DataFusionError::Execution(format!("Failed to send job: {}", err)))
            }
        }
    }

    async fn receive_job(&self) -> MizuDataSinkJob {
        self.receiver.lock().expect("Failed to lock receiver").recv().await.unwrap()
    }

    async fn start_background_sink_job(&self) {
        loop {
            let job = self.receive_job().await;
            info!("Received job");
            self.process_job(job).await;
        }
    }

    async fn process_job(&self, job: MizuDataSinkJob) {
        match job {
            MizuDataSinkJob::BufferWrite { mut record_batch, context, buffer, completion } => {
                let mut count: u64 = 0;
                let mut records = Vec::new();
                match buffer.lock() {
                    Ok(mut buffer) => {
                        for record in record_batch.next().await {
                            if let Err(err) = record {
                                completion.send(Err(err)).unwrap();
                                return;
                            } else {
                                records.push(record.unwrap());
                                count += 1;
                            }
                        }

                        buffer.push(MizuDataSinkBuffer { records, context });
                    }
                    Err(e) => {
                        completion.send(Err(DataFusionError::Execution(format!("Failed to lock buffer: {}", e)))).unwrap();
                        return;
                    }
                }
                completion.send(Ok(count)).unwrap();
            }
            _ => unimplemented!(),
        }
    }
}