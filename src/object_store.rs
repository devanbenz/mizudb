use async_trait::async_trait;
use bytes::Bytes;
use chrono::TimeZone;
use datafusion::arrow::compute::concat_batches;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::object_store::path::Path;
use datafusion::object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use datafusion::parquet::arrow::arrow_reader::{
    ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::arrow::ArrowWriter;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, RwLock};
use t4::MountOptions;
use url::Url;
use wincode::{SchemaRead, SchemaWrite};

const METADATA_CACHE_FILE: &str = "metadata_cache.bin";

#[derive(SchemaRead, SchemaWrite)]
struct MizuObjectStoreSerializerInner {
    key: String,
    location: String,
    last_modified: i64,
    size: u64,
}

#[derive(SchemaRead, SchemaWrite)]
struct MizuObjectStoreSerializer {
    inner: Vec<MizuObjectStoreSerializerInner>,
}

struct MizuObjectStoreInner {
    /// db_file is a map of table names to ObjectMeta.
    db_file: Arc<RwLock<HashMap<String, ObjectMeta>>>,
}

pub struct MizuObjectStore {
    store: Arc<t4::Store>,
    inner: MizuObjectStoreInner,
}

impl MizuObjectStore {
    pub(crate) async fn new(path: &str) -> t4::Result<Self> {
        let opts = MountOptions {
            queue_depth: 256,
            direct_io: false,
            dsync: true,
        };
        let store = t4::mount_with_options(path, opts).await?;

        Ok(Self {
            store: Arc::new(store),
            inner: MizuObjectStoreInner {
                db_file: Arc::new(RwLock::new(HashMap::new())),
            },
        })
    }

    pub async fn load_meta(&self) {
        if let Ok(metadata_cache) = self.store.get(METADATA_CACHE_FILE.as_bytes()).await {
            self.deserialize_meta_cache(&metadata_cache);
        }
    }

    pub async fn load_catalog(&self) -> Option<Vec<u8>> {
        let catalog = self.store.get("catalog.parquet".as_bytes()).await;
        if let Ok(catalog) = catalog {
            Some(catalog.to_vec())
        } else {
            None
        }
    }

    pub async fn get_metadata(&self, key: &str) -> Option<Vec<u8>> {
        let metadata = self.store.get(key.as_bytes()).await;
        if let Ok(metadata) = metadata {
            Some(metadata.to_vec())
        } else {
            None
        }
    }

    pub fn get_db_cache(&self) {
        if self.inner.db_file.read().unwrap().is_empty() {
            println!("No DB cache in file");
        }
        for (k, v) in self.inner.db_file.read().unwrap().iter() {
            println!("key {}, value {:#?}", k, v);
        }
    }

    pub async fn get_raw_bytes(&self, key: &str) -> Vec<u8> {
        self.store.get(key.as_ref()).await.unwrap().to_vec()
    }

    fn serialize_meta_cache(&self) -> Vec<u8> {
        let mut inner = Vec::new();
        for (k, v) in self.inner.db_file.read().unwrap().iter() {
            let v = MizuObjectStoreSerializerInner {
                key: k.to_string(),
                location: v.location.to_string(),
                last_modified: v.last_modified.timestamp(),
                size: v.size,
            };
            inner.push(v);
        }

        let outer = MizuObjectStoreSerializer { inner };
        let serialized = wincode::serialize(&outer).unwrap();
        serialized
    }

    fn deserialize_meta_cache(&self, serialized: &[u8]) {
        let outer: MizuObjectStoreSerializer = wincode::deserialize(serialized).unwrap();
        for v in outer.inner {
            let meta = ObjectMeta {
                location: Path::parse(&v.location).unwrap(),
                last_modified: chrono::Utc.timestamp_opt(v.last_modified, 0).unwrap(),
                size: v.size,
                e_tag: None,
                version: None,
            };
            self.inner.db_file.write().unwrap().insert(v.key, meta);
        }
    }

    // TODO: This is not scalable at all, I'm sure there's a better way to do this.
    async fn merge(&self, input_data: PutPayload, key: &str) -> t4::Result<Bytes> {
        let mut buf: Vec<u8> = Vec::new();
        let existing_data = self.store.get(key.as_bytes()).await?;
        let existing_data = Bytes::from(existing_data);
        let existing_data_reader =
            ParquetRecordBatchReaderBuilder::try_new(existing_data).expect("parquet reader");
        let existing_schema = existing_data_reader.schema().clone();
        let existing_data_reader = existing_data_reader.build().expect("parquet reader");
        let mut writer =
            ArrowWriter::try_new(&mut buf, existing_schema.clone(), None).expect("arrow writer");
        let mut batches = existing_data_reader
            .map(|batch| batch.expect("batch"))
            .collect::<Vec<_>>();

        for data in input_data.into_iter() {
            let new_data_reader =
                ParquetRecordBatchReader::try_new(data, 100).expect("parquet reader");
            for batch in new_data_reader {
                batches.push(batch.expect("batch"));
            }
        }

        let batches = concat_batches(&existing_schema, &batches).expect("concat batches");
        writer.write(&batches).expect("write");

        writer.close().expect("close writer");
        Ok(Bytes::from(buf))
    }
}

impl Debug for MizuObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuObjectStore {{ inner: {:?} }}", self.store)
    }
}

impl Display for MizuObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuObjectStore")
    }
}

#[async_trait]
impl ObjectStore for MizuObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        _: PutOptions,
    ) -> datafusion::object_store::Result<PutResult> {
        let file = location.filename().expect("location must have a filename");
        let mut meta = ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size: payload.content_length() as u64,
            e_tag: None,
            version: None,
        };

        let dblocation = {
            let db_file = self.inner.db_file.read().unwrap();
            let db_meta = db_file.get(file);
            if let Some(db_meta) = db_meta {
                db_meta.location.filename().map(|f| f.to_string())
            } else {
                None
            }
        };

        let merged_data = {
            if let Some(dblocation) = &dblocation {
                let merged = self.merge(payload, &dblocation).await.map_err(|err| {
                    datafusion::object_store::Error::Generic {
                        store: "",
                        source: Box::new(err),
                    }
                })?;
                self.store.remove(dblocation.as_bytes()).await.unwrap();
                self.store.sync().await.unwrap();
                meta.size = merged.len() as u64;
                merged
            } else {
                payload.into_iter().next().unwrap()
            }
        };

        // Write the payload as one contiguous value: chunk-wise puts under
        // the same key would each overwrite the previous chunk.
        self.store
            .put(file, merged_data.to_vec())
            .await
            .map_err(|err| datafusion::object_store::Error::Generic {
                store: "",
                source: Box::new(err),
            })?;
        self.inner.db_file
            .write()
            .unwrap()
            .insert(file.parse().unwrap(), meta.clone());

        self.store.put(METADATA_CACHE_FILE, self.serialize_meta_cache()).await.unwrap();
        self.store.sync().await.unwrap();

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> datafusion::object_store::Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> datafusion::object_store::Result<GetResult> {
        let file = location.filename().expect("location must have a filename");
        let meta = self
            .inner
            .db_file
            .read()
            .unwrap()
            .get(file)
            .filter(|meta| meta.location == *location)
            .cloned()
            .ok_or_else(|| datafusion::object_store::Error::NotFound {
                path: location.to_string(),
                source: format!("no db file for table {file}").into(),
            })?;

        // Read parquet footer
        let range = match &options.range {
            Some(range) => range.as_range(meta.size).map_err(|err| {
                datafusion::object_store::Error::Generic {
                    store: "",
                    source: Box::new(err),
                }
            })?,
            None => 0..meta.size,
        };

        let data = self
            .store
            .get_range(file.as_bytes(), range.start, range.end - range.start)
            .await
            .map_err(|err| datafusion::object_store::Error::Generic {
                store: "",
                source: Box::new(err),
            })?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::iter(vec![Ok(bytes::Bytes::from(data))]).boxed(),
            ),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        _locations: futures_core::stream::BoxStream<
            'static,
            datafusion::object_store::Result<Path>,
        >,
    ) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<Path>> {
        todo!()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<ObjectMeta>>
    {
        let metas: Vec<_> = self
            .inner
            .db_file
            .read()
            .unwrap()
            .values()
            .filter(|meta| match prefix {
                Some(prefix) => meta.location.prefix_match(prefix).is_some(),
                None => true,
            })
            .cloned()
            .map(Ok)
            .collect();
        futures_util::stream::iter(metas).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> datafusion::object_store::Result<ListResult> {
        todo!()
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> datafusion::object_store::Result<()> {
        todo!()
    }
}

fn get_url_key(url: &Url) -> String {
    url[url::Position::BeforeScheme..url::Position::BeforePath].to_string()
}

pub struct MizuObjectStoreRegistry {
    stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    default_store: Option<Arc<dyn ObjectStore>>,
    default_store_url: Option<Url>,
}

impl MizuObjectStoreRegistry {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            default_store: None,
            default_store_url: None,
        }
    }

    pub fn with_default_store(store: Arc<dyn ObjectStore>, url: Url) -> Self {
        let mut stores = HashMap::new();
        stores.insert(get_url_key(&url), store.clone());
        Self {
            stores: RwLock::new(stores),
            default_store: Some(store),
            default_store_url: Some(url),
        }
    }
}

impl Debug for MizuObjectStoreRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MizuObjectStoreRegistry {{ stores: {:?}, default_store: {:?}, default_store_url: {:?} }}",
            self.stores, self.default_store, self.default_store_url
        )
    }
}

impl ObjectStoreRegistry for MizuObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.stores
            .write()
            .unwrap()
            .insert(get_url_key(url), store.clone());
        Some(store)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.read().unwrap().get(&get_url_key(url)) {
            Ok(store.clone())
        } else if let Some(store) = &self.default_store {
            Ok(store.clone())
        } else {
            Err(DataFusionError::Execution(format!(
                "No store registered for {}",
                url
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::object_store::memory::InMemory;

    #[test]
    fn get_store_ignores_path_on_registered_url() {
        let registry = MizuObjectStoreRegistry::with_default_store(
            Arc::new(InMemory::new()),
            Url::parse("file:///tmp/datafusion_tmp/").unwrap(),
        );

        registry
            .get_store(&Url::parse("file:///").unwrap())
            .expect("store registered with a path should resolve for file:///");
    }

    #[test]
    fn register_store_normalizes_key() {
        let registry = MizuObjectStoreRegistry::new();
        registry.register_store(
            &Url::parse("s3://bucket/some/prefix/").unwrap(),
            Arc::new(InMemory::new()),
        );

        registry
            .get_store(&Url::parse("s3://bucket").unwrap())
            .expect("store should resolve by scheme://authority");
    }
}
