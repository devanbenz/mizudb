mod catalog;
mod object_store;
mod table;
mod wal;

use crate::catalog::{MizuCatalog, MizuSchemaProvider};
use crate::object_store::{MizuObjectStore, MizuObjectStoreRegistry};
use crate::table::MizuTable;
use bytes::Bytes;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::CatalogProvider;
use datafusion::common::{DFSchema, HashMap};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    DdlStatement, DmlStatement, EmptyRelation, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::object_store::ObjectStore;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{col, lit, SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::TableReference;
use futures_util::StreamExt;
use std::fs::exists;
use std::io::Write;
use std::sync::{Arc, RwLock};
use url::Url;

const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_CATALOG: &str = "mizudb";

struct MizuDB {
    catalog: Arc<dyn CatalogProvider>,
    catalog_table_provider: Arc<dyn TableProvider>,
    session_ctx: SessionContext,
    table_path: String,
    runtime_env: Arc<RuntimeEnv>,
    /// database_table_providers is a map of database name to table providers.
    /// The key is the name of the database, and the value is the table provider.
    /// This ensures we have a single file for each database.
    database_table_providers_cache: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
    object_store: Arc<MizuObjectStore>,
}

impl MizuDB {
    async fn new(db_path: String) -> datafusion::error::Result<Self> {
        let path = format!("{}/mizudb_store", db_path);
        let table_path = format!("file://{}", path.as_str());
        let catalog = Arc::new(MizuCatalog::new());
        let object_store = Arc::new(
            MizuObjectStore::new(path.as_str())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let object_store_registry = Arc::new(MizuObjectStoreRegistry::with_default_store(
            object_store.clone(),
            Url::try_from(table_path.as_str()).unwrap(),
        ));

        let catalog_schema = SchemaRef::new(Schema::new(vec![
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
        ]));
        let catalog_file_source = Arc::new(ParquetSource::new(catalog_schema.clone()));
        let catalog_file_table_path =
            ListingTableUrl::parse(&format!("{}/catalog.parquet", table_path));
        let catalog_table_provider = Arc::new(MizuTable::new(
            catalog_schema.clone(),
            ObjectStoreUrl::parse("file://")?,
            catalog_file_source.clone(),
            catalog_file_table_path?,
        ));

        let rt = Arc::new(
            RuntimeEnvBuilder::default()
                .with_object_store_registry(object_store_registry)
                .build()?,
        );
        let session_config =
            SessionConfig::new().with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);
        let session_ctx = SessionContext::new_with_config_rt(session_config, rt.clone());

        session_ctx.register_catalog(DEFAULT_CATALOG, catalog.clone());
        catalog.register_schema(DEFAULT_SCHEMA, Arc::new(MizuSchemaProvider::new()))?;
        let table_ref = TableReference::full(DEFAULT_CATALOG, DEFAULT_SCHEMA, "mizudb_store");
        session_ctx.register_table(table_ref.clone(), catalog_table_provider.clone())?;

        // TODO: Check if db file exists, if not create it, if it does load it in to the catalog.
        if exists(path.as_str())? {
            // Self::load_db(catalog).await
            Ok(Self {
                runtime_env: Arc::clone(&rt),
                catalog,
                catalog_table_provider,
                session_ctx,
                table_path,
                database_table_providers_cache: Arc::new(Default::default()),
                object_store,
            })
        } else {
            Ok(Self {
                runtime_env: Arc::clone(&rt),
                catalog,
                catalog_table_provider,
                session_ctx,
                table_path,
                database_table_providers_cache: Arc::new(Default::default()),
                object_store,
            })
        }
    }

    fn ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }

    async fn load_db(
        catalog: Arc<dyn CatalogProvider>,
        catalog_table_provider: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            runtime_env: Arc::new(RuntimeEnv::default()),
            catalog,
            catalog_table_provider,
            session_ctx: SessionContext::new(),
            table_path: "".to_string(),
            database_table_providers_cache: Arc::new(Default::default()),
            object_store: Arc::new(MizuObjectStore::new("").await.unwrap()),
        })
    }

    async fn files(&self) -> Vec<String> {
        let mut metas = self.object_store.list(None);
        while let Some(data) = metas.next().await {
            match data {
                Ok(data) => {
                    let metadata = self
                        .object_store
                        .get_metadata(data.location.as_ref())
                        .await
                        .unwrap();
                    let metadata_bytes = Bytes::from(metadata);
                    let mut metpq = ParquetRecordBatchReader::try_new(metadata_bytes, 100).unwrap();
                    while let Some(record_batch) = metpq.next() {
                        println!("{:#?}", record_batch);
                    }
                }
                Err(e) => {
                    println!("{:#?}", e);
                }
            }
        }

        vec![]
    }

    // TODO: Refactor exec
    // - Table and Schema creation should be less verbose and in private methods
    async fn exec(&self, stmt: Statement) -> datafusion::error::Result<DataFrame> {
        match self.ctx().state().statement_to_plan(stmt).await? {
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(stmt) => {
                    let schema = stmt.name.schema().or_else(|| Some(DEFAULT_SCHEMA)).unwrap();
                    let schema_ref = stmt.input.schema();

                    let table_provider = match self.database_table_providers_cache.write() {
                        Ok(mut table_provider) => table_provider
                            .entry(stmt.name.table().to_string())
                            .or_insert(Arc::new(MizuTable::new(
                                schema_ref.inner().clone(),
                                ObjectStoreUrl::parse("file://")?,
                                Arc::from(ParquetSource::new(schema_ref.inner().clone())),
                                ListingTableUrl::parse(&format!(
                                    "{}/{}.parquet",
                                    self.table_path,
                                    stmt.name.table()
                                ))?,
                            )))
                            .clone(),
                        Err(err) => {
                            panic!("Error reading table provider cache: {:?}", err);
                        }
                    };

                    let table_source =
                        Arc::new(DefaultTableSource::new(self.catalog_table_provider.clone()));
                    let catalog_input = LogicalPlanBuilder::values(vec![vec![
                        lit(schema),
                        lit(stmt.name.table()),
                    ]])?
                        .project(vec![
                            col("column1").alias("schema_name"),
                            col("column2").alias("table_name"),
                        ])?
                        .build()?;
                    let logical_plan = LogicalPlanBuilder::insert_into(
                        catalog_input,
                        TableReference::full(DEFAULT_CATALOG, DEFAULT_SCHEMA, "mizudb_store"),
                        table_source,
                        InsertOp::Append,
                    )?
                        .build()?;
                    let physical_plan = self
                        .ctx()
                        .state()
                        .create_physical_plan(&logical_plan)
                        .await?;
                    let stream =
                        execute_stream(physical_plan.clone(), self.ctx().task_ctx().clone())?;
                    let streams = collect(stream).await?;
                    for stream in streams {
                        println!("{:#?}", stream);
                    }
                    self.ctx()
                        .register_table(stmt.name.clone(), table_provider.clone())?;
                    Ok(Self::empty_df_ok(
                        self.ctx().clone().state(),
                        schema_ref.clone(),
                    )?)
                }
                DdlStatement::CreateCatalogSchema(stmt) => {
                    // TODO: Create the schema on disk in the catalog
                    let parsed = TableReference::from(stmt.schema_name.as_str());
                    let schema_name = parsed.table();
                    let provider = Arc::new(MizuSchemaProvider::new());
                    self.catalog().register_schema(schema_name, provider)?;
                    println!("Schemas {:?}", self.catalog.clone().schema_names());
                    Ok(Self::empty_df_ok(
                        self.ctx().clone().state(),
                        stmt.schema.clone(),
                    )?)
                }
                _ => unimplemented!(),
            },
            LogicalPlan::Dml(dml) => match dml {
                DmlStatement {
                    table_name,
                    target,
                    op,
                    input,
                    output_schema,
                } => {
                    let logical_input = input.as_ref().clone();
                    let logical_plan = LogicalPlanBuilder::insert_into(
                        logical_input,
                        table_name,
                        target,
                        InsertOp::Append,
                    )?
                        .build()?;
                    let physical_plan = self
                        .ctx()
                        .state()
                        .create_physical_plan(&logical_plan)
                        .await?;
                    let stream =
                        execute_stream(physical_plan.clone(), self.ctx().task_ctx().clone())?;
                    let streams = collect(stream).await?;
                    for stream in streams {
                        println!("{:#?}", stream);
                    }
                    Ok(Self::empty_df_ok(
                        self.ctx().clone().state(),
                        output_schema.clone(),
                    )?)
                }
            },
            plan => Ok(self.ctx().execute_logical_plan(plan).await?),
        }
    }

    fn empty_df_ok(
        session_state: SessionState,
        schema: Arc<DFSchema>,
    ) -> datafusion::error::Result<DataFrame> {
        Ok(DataFrame::new(
            session_state,
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.clone(),
            }),
        ))
    }
}

fn prompt(text: &str) -> String {
    print!("{} ", text);
    std::io::stdout().flush().expect("error flushing stdout");

    let mut response = String::new();
    std::io::stdin()
        .read_line(&mut response)
        .expect("failed to get input");

    response.trim_end().to_string()
}

const DEFAULT_DB_PATH: &str = "/tmp/mizu_store";

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_DB_PATH.to_string());
    let db = MizuDB::new(path).await?;
    loop {
        let input = prompt("> ");
        if input == "exit" {
            break;
        }
        if input == "files" {
            let files = db.files().await;
            for file in files {
                println!("{}", file);
            }
            continue;
        }
        match DFParser::parse_sql(&input) {
            Ok(statements) => {
                for s in statements {
                    let df = db.exec(s).await;
                    if let Err(e) = df {
                        println!("Execution error: {}", e);
                    } else {
                        df?.show().await?;
                    }
                }
            }
            Err(err) => {
                println!("SQL parsing error: {}", err);
                continue;
            }
        }
    }

    Ok(())
}
