use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreRegistry;
use datafusion::object_store::path::Path;
use datafusion::object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, RwLock};
use url::Url;

pub struct MizuObjectStore {
    inner: Arc<t4::Store>,
}

impl MizuObjectStore {
    async fn new(path: &str) -> t4::Result<Self> {
        let store = t4::mount(path).await?;
        Ok(Self {
            inner: Arc::new(store),
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
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> datafusion::object_store::Result<PutResult> {
        todo!()
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> datafusion::object_store::Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> datafusion::object_store::Result<GetResult> {
        todo!()
    }

    fn delete_stream(&self, locations: futures_core::stream::BoxStream<'static, datafusion::object_store::Result<Path>>) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<Path>> {
        todo!()
    }

    fn list(&self, prefix: Option<&Path>) -> futures_core::stream::BoxStream<'static, datafusion::object_store::Result<ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> datafusion::object_store::Result<ListResult> {
        todo!()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> datafusion::object_store::Result<()> {
        todo!()
    }
}

pub struct MizuObjectStoreRegistry {
    stores: RwLock<HashMap<Url, Arc<dyn ObjectStore>>>,
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
        stores.insert(url.clone(), store.clone());
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
        self.stores.write().unwrap().insert(url.clone(), store.clone());
        Some(store)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        if let Some(store) = self.stores.read().unwrap().get(url) {
            Ok(store.clone())
        } else {
            Err(DataFusionError::Execution(format!("No store registered for {}", url)))
        }
    }
}

