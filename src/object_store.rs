use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::object_store::path::Path;
use datafusion::object_store::{CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, RwLock};
use url::Url;

pub struct MizuObjectStore {
    inner: Arc<t4::Store>,
    indices: Arc<RwLock<HashMap<String, Vec<usize>>>>,
    // One parquet file per database: keyed by the first path segment
    // (the database), so a new put for a database replaces its file.
    db_file: Arc<RwLock<HashMap<String, ObjectMeta>>>,
}

impl MizuObjectStore {
    pub(crate) async fn new(path: &str) -> t4::Result<Self> {
        let store = t4::mount(path).await?;
        Ok(Self {
            inner: Arc::new(store),
            indices: Arc::new(RwLock::new(HashMap::new())),
            db_file: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

impl Debug for MizuObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuObjectStore {{ inner: {:?} }}", self.inner)
    }
}

impl Display for MizuObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuObjectStore")
    }
}

#[async_trait]
impl ObjectStore for MizuObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, _: PutOptions) -> datafusion::object_store::Result<PutResult> {
        println!("Putting {:#?} to {}", payload, location);
        let database = location
            .parts()
            .next()
            .map(|part| part.as_ref().to_string())
            .unwrap_or_default();
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: chrono::Utc::now(),
            size: payload.content_length() as u64,
            e_tag: None,
            version: None,
        };

        let dblocation = {
            let db_file = self.db_file.read().unwrap();
            let db_meta = db_file.get(&database);
            if let Some(db_meta) = db_meta {
                Some(db_meta.location.to_string())
            } else {
                None
            }
        };

        if let Some(dblocation) = dblocation {
            self.inner.get(dblocation.as_bytes()).await.map(|data| {
                println!("Existing data: {:#?}", data);
            })
                .map_err(|err| {
                    datafusion::object_store::Error::Generic {
                        store: "",
                        source: Box::new(err),
                    }
                })?;
        }

        // Write the payload as one contiguous value: chunk-wise puts under
        // the same key would each overwrite the previous chunk.
        let data = bytes::Bytes::from(payload);
        self.inner.put(location.to_string(), data.to_vec()).await.map_err(|err| {
            datafusion::object_store::Error::Generic {
                store: "",
                source: Box::new(err),
            }
        })?;
        self.db_file.write().unwrap().insert(database, meta.clone());

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> datafusion::object_store::Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> datafusion::object_store::Result<GetResult> {
        let database = location
            .parts()
            .next()
            .map(|part| part.as_ref().to_string())
            .unwrap_or_default();
        let meta = self
            .db_file
            .read()
            .unwrap()
            .get(&database)
            .filter(|meta| meta.location == *location)
            .cloned()
            .ok_or_else(|| datafusion::object_store::Error::NotFound {
                path: location.to_string(),
                source: format!("no db file for database {database}").into(),
            })?;

        // Parquet reads come in as ranged gets (footer first), so the
        // requested range must be honored, not the whole object returned.
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
            .inner
            .get_range(location.to_string().as_bytes(), range.start, range.end - range.start)
            .await
            .map_err(|err| datafusion::object_store::Error::Generic {
                store: "",
                source: Box::new(err),
            })?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::iter(vec![Ok(bytes::Bytes::from(data))]).boxed()
            ),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    fn delete_stream(&self, locations: futures_core::stream::BoxStream<'static, datafusion::object_store::Result<Path>>) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<Path>> {
        todo!()
    }

    fn list(&self, prefix: Option<&Path>) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<ObjectMeta>> {
        let metas: Vec<_> = self
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

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> datafusion::object_store::Result<ListResult> {
        todo!()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> datafusion::object_store::Result<()> {
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
    default_store_path: Option<String>,
}

impl MizuObjectStoreRegistry {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            default_store: None,
            default_store_url: None,
            default_store_path: None,
        }
    }

    pub fn with_default_store(store: Arc<dyn ObjectStore>, url: Url, path: String) -> Self {
        let mut stores = HashMap::new();
        stores.insert(get_url_key(&url), store.clone());
        Self {
            stores: RwLock::new(stores),
            default_store: Some(store),
            default_store_url: Some(url),
            default_store_path: Some(path),
        }
    }
}

impl Debug for MizuObjectStoreRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuObjectStoreRegistry {{ stores: {:?}, default_store: {:?}, default_store_url: {:?}, default_store_path: {:?} }}", self.stores, self.default_store, self.default_store_url, self.default_store_path)
    }
}

impl ObjectStoreRegistry for MizuObjectStoreRegistry {
    fn register_store(&self, url: &Url, store: Arc<dyn ObjectStore>) -> Option<Arc<dyn ObjectStore>> {
        self.stores.write().unwrap().insert(get_url_key(url), store.clone());
        Some(store)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.read().unwrap().get(&get_url_key(url)) {
            Ok(store.clone())
        } else if let Some(store) = &self.default_store {
            Ok(store.clone())
        } else {
            Err(DataFusionError::Execution(format!("No store registered for {}", url)))
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
            "".to_string(),
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

