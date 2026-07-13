use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::object_store::ObjectMeta;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};

pub struct CatalogEntries {
    schema_table_entries: Vec<MizuCatalogEntry>,
    object_meta_entries: Vec<MizuCatalogObjectMeta>,
}

pub struct MizuCatalogObjectMeta {
    table_name: String,
    object_meta: ObjectMeta,
}

pub struct MizuCatalogEntry {
    schema_name: String,
    table_name: String,
}

// TODO: Catalog needs to implement datafusion json datatypes
pub struct MizuSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl MizuSchemaProvider {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
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

    async fn table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.tables.read().unwrap().get(name) {
            Ok(Some(table.clone()))
        } else {
            Ok(None)
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        println!("Registering table {} with schema {}", name, table.schema());
        let mut tables = self.tables.write().unwrap();
        tables.insert(name, table.clone());
        Ok(Some(table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().unwrap().contains_key(name)
    }
}

pub struct MizuCatalog {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MizuCatalog {
    pub(crate) fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) async fn get_schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(name).cloned()
    }
}

impl Debug for MizuCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MizuCatalog {{ schemas: {:?} }}", self.schemas)
    }
}

impl CatalogProvider for MizuCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.read().unwrap().keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.read().unwrap().get(name).cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self
            .schemas
            .write()
            .unwrap()
            .insert(name.to_string(), schema))
    }
}
