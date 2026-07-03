mod object_store;

use crate::object_store::MizuObjectStoreRegistry;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};
use url::Url;

pub struct MizuSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl Debug for MizuSchemaProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuSchemaProvider {{ tables: {:?} }}", self.tables)
    }
}

#[async_trait]
impl SchemaProvider for MizuSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables.read().unwrap().get(name) {
            Ok(Some(table.clone()))
        } else {
            Ok(None)
        }
    }

    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        tables.insert(name, table.clone());
        Ok(Some(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().unwrap().contains_key(name)
    }
}

pub struct MizuCatalog {
    tables: Vec<String>,
    files: Vec<String>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl MizuCatalog {
    fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            tables: vec![],
            files: vec![],
        }
    }
}

impl Debug for MizuCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuCatalog {{ tables: {:?}, files: {:?}, schemas: {:?} }}", self.tables, self.files, self.schemas)
    }
}

impl CatalogProvider for MizuCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }

    fn register_schema(&self, name: &str, schema: Arc<dyn SchemaProvider>) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schema_map = self.schemas.clone();
        schema_map.insert(name.to_string(), schema.clone());
        Ok(Some(schema))
    }
}

pub async fn bootstrap_rt_ctx() -> datafusion::error::Result<Arc<SessionContext>> {
    let object_store_registry = Arc::new(MizuObjectStoreRegistry::with_default_store(Arc::new(InMemory::new()), Url::try_from("file://").unwrap(), "".to_string()));
    let rt = RuntimeEnvBuilder::default().with_object_store_registry(object_store_registry).build()?;

    let session_config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(rt));
    let catalog = Arc::new(MizuCatalog::new());
    ctx.register_catalog("mizu", catalog.clone());
    ctx.sql("CREATE SCHEMA mizu").await?;
    ctx.sql("CREATE TABLE mizu.test (a int)").await?;
    ctx.sql("INSERT INTO mizu.test VALUES (1)").await?;
    ctx.sql("SELECT * FROM mizu.test").await?;
    println!("{:?}", catalog.schema_names());

    Ok(Arc::new(ctx))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = bootstrap_rt_ctx().await?;
    println!("Hello, world!");

    Ok(())
}
