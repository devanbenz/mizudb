use crate::wal::MizuWAL;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

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
        schema: SchemaRef,
        completion: oneshot::Sender<datafusion::common::Result<u64>>,
    },
}

type MizuDataSinkName = String;

// TODO: Implement other types of sinks
pub struct MizuDiskManagerCacheEntry {
    wal: Arc<MizuWAL>,
    parquet_sink: Arc<ParquetSink>,
    record_batches: Vec<RecordBatch>,
    schema: SchemaRef,
    bytes_size: usize,
}

impl MizuDiskManagerCacheEntry {
    pub fn new(wal: Arc<MizuWAL>, parquet_sink: Arc<ParquetSink>, schema: SchemaRef, bytes: usize) -> Self {
        Self {
            wal,
            parquet_sink,
            record_batches: vec![],
            schema,
            bytes_size: bytes,
        }
    }
}

const DEFAULT_NUM_THREADS: usize = 4;

pub(crate) struct MizuDiskManager {
    sender: tokio::sync::mpsc::Sender<MizuDataSinkJob>,
    cache: Arc<Mutex<HashMap<MizuDataSinkName, MizuDiskManagerCacheEntry>>>,
}

impl MizuDiskManager {
    pub async fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<MizuDataSinkJob>(DEFAULT_NUM_THREADS * 2);
        let dm = Self {
            sender: tx,
            cache: Arc::new(Mutex::new(HashMap::new())),
        };
        tokio::spawn(Self::start_background_sink_job(rx, dm.cache.clone()));
        dm
    }

    pub(crate) fn get_wal(&self, name: &str) -> Arc<MizuWAL> {
        self.cache.lock().unwrap().get(name).unwrap().wal.clone()
    }


    pub(crate) fn insert_if_not_exists(&self, name: &str, entry: MizuDiskManagerCacheEntry) {
        self.cache.lock().unwrap().entry(name.to_string()).or_insert(entry);
    }

    pub(crate) fn get_parquet_sink(&self, name: &str) -> Arc<ParquetSink> {
        self.cache
            .lock()
            .unwrap()
            .get(name)
            .unwrap()
            .parquet_sink
            .clone()
    }

    pub(crate) async fn send_job(&self, job: MizuDataSinkJob) -> datafusion::common::Result<()> {
        println!("Sending job");
        match self.sender.clone().send(job).await {
            Ok(_) => Ok(()),
            Err(err) => Err(DataFusionError::Execution(format!(
                "Failed to send job: {}",
                err
            ))),
        }
    }

    pub(crate) async fn start_background_sink_job(
        mut recv: Receiver<MizuDataSinkJob>,
        cache: Arc<Mutex<HashMap<MizuDataSinkName, MizuDiskManagerCacheEntry>>>,
    ) {
        println!("Starting background job worker");
        while let Some(item) = recv.recv().await {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                Self::process_job(item, cache).await;
            });
        }
    }

    async fn process_job(
        job: MizuDataSinkJob,
        cache: Arc<Mutex<HashMap<MizuDataSinkName, MizuDiskManagerCacheEntry>>>,
    ) {
        match job {
            MizuDataSinkJob::BufferWrite {
                mut record_batch,
                context,
                completion,
                schema,
            } => {
                let mut record_batches = vec![];
                let mut count: u64 = 0;
                while let Some(record) = record_batch.next().await {
                    if let Err(err) = record {
                        completion.send(Err(err)).unwrap();
                        return;
                    } else {
                        println!("{:#?}", record.iter().clone().collect::<Vec<_>>());
                        record_batches.push(record.unwrap());
                        count += 1;
                    }
                }

                completion.send(Ok(count)).unwrap();
            }
            MizuDataSinkJob::WALWrite {
                mut record_batch,
                context,
            } => {
                let mut record_batches = vec![];
                while let Some(record) = record_batch.next().await {
                    if let Err(err) = record {
                        println!("Error: {}", err);
                        return;
                    } else {
                        record_batches.push(record.unwrap());
                    }
                }
                let mut parquet_sink = cache.lock().unwrap().get_mut("parquet.parquet").unwrap().parquet_sink.clone();
            }
            _ => todo!()
        }
    }
}
