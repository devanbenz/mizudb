use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

struct MizuSchemaProvider {
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

struct MizuCatalog {
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

async fn bootstrap_rt_ctx() -> datafusion::error::Result<Arc<SessionContext>> {
    let rt = RuntimeEnvBuilder::default().build()?;
    // let url = url::Url::try_from("").unwrap();
    // rt.register_object_store(&url, Arc::new(InMemory::new()));

    let session_config = SessionConfig::new();
    let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(rt));
    ctx.register_parquet("test", "<TODO>", ParquetReadOptions::new()).await?;

    let catalog = MizuCatalog::new();
    let table_path = ListingTableUrl::parse("iris.parquet").unwrap();
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    let config = ListingTableConfig::new(table_path).with_listing_options(listing_options).infer_schema(&ctx.state()).await?;
    let table_provider = Arc::new(ListingTable::try_new(config)?);


    Ok(Arc::new(ctx))
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = bootstrap_rt_ctx().await?;
    println!("Hello, world!");

    Ok(())
}
