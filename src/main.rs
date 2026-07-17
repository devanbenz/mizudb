mod catalog;
mod object_store;
mod table;
mod wal;
mod data_sink;
mod disk_manager;

use crate::catalog::{MizuCatalog, MizuSchemaProvider};
use crate::disk_manager::MizuDiskManager;
use crate::object_store::{MizuObjectStore, MizuObjectStoreRegistry};
use crate::table::MizuTable;
use bytes::Bytes;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::CatalogProvider;
use datafusion::common::{DFSchema, HashMap};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
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
use json::JsonValue;
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
    /// database_table_providers is a map of database name to table providers.
    /// The key is the name of the database, and the value is the table provider.
    /// This ensures we have a single file for each database.
    database_table_providers_cache: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
    object_store: Arc<MizuObjectStore>,
    disk_manager: Arc<MizuDiskManager>,
}

// TODO: Refactoring time!
impl MizuDB {
    async fn new(db_path: String) -> datafusion::error::Result<Self> {
        let path = format!("{}/mizudb_store", db_path);
        let table_path = format!("file://{}", path.as_str());
        let disk_manager = Arc::new(MizuDiskManager::new().await);
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
            Field::new("schema", DataType::Utf8, false),
        ]));
        let catalog_file_source = Arc::new(ParquetSource::new(catalog_schema.clone()));
        let catalog_file_table_path =
            ListingTableUrl::parse(&format!("{}/catalog.parquet", table_path));
        let catalog_table_provider = Arc::new(MizuTable::new(
            catalog_schema.clone(),
            ObjectStoreUrl::parse("file://")?,
            catalog_file_source.clone(),
            catalog_file_table_path?,
            disk_manager.clone(),
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

        // TODO: Refactor this and make it more modular
        if exists(path.as_str())? {
            let db = Self {
                catalog: catalog.clone(),
                catalog_table_provider,
                session_ctx: session_ctx.clone(),
                table_path,
                database_table_providers_cache: Arc::new(Default::default()),
                object_store: object_store.clone(),
                disk_manager,
            };

            if let Some(cat) = object_store.load_catalog().await {
                let reader = ParquetRecordBatchReader::try_new(Bytes::from(cat), 10)
                    .expect("ParquetRecordBatchReader");

                for value in reader {
                    let value = value?;
                    let schema_name = value
                        .column(0)
                        .clone()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone();
                    let table_name = value
                        .column(1)
                        .clone()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone();
                    let schema = value
                        .column(2)
                        .clone()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone();

                    for idx in 0..schema_name.len() {
                        catalog.register_schema(
                            schema_name.value(idx),
                            Arc::new(MizuSchemaProvider::new()),
                        )?;
                        let schema_json =
                            json::parse(schema.value(idx)).expect("catalog schema is valid json");
                        let mut schema_fields = vec![];
                        for (key, val) in schema_json.entries() {
                            let dt: DataType = val
                                .as_str()
                                .expect("")
                                .parse()
                                .expect("Could not parse datatype");
                            schema_fields.push(Field::new(key, dt, false));
                        }
                        let schema = SchemaRef::new(Schema::new(schema_fields));
                        let table_provider = db.get_table_provider(
                            schema_name.value(idx),
                            table_name.value(idx),
                            &schema,
                        );
                        let table_ref = TableReference::full(
                            DEFAULT_CATALOG,
                            schema_name.value(idx),
                            table_name.value(idx),
                        );
                        session_ctx.register_table(table_ref, table_provider.clone())?;
                    }
                }
                object_store.load_meta().await;
            }

            Ok(db)
        } else {
            Ok(Self {
                catalog,
                catalog_table_provider,
                session_ctx,
                table_path,
                database_table_providers_cache: Arc::new(Default::default()),
                object_store,
                disk_manager,
            })
        }
    }

    fn ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
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

    fn get_table_provider(
        &self,
        schema_name: &str,
        table_name: &str,
        schema_ref: &SchemaRef,
    ) -> Arc<dyn TableProvider> {
        let file_stem = format!("{}_{}", schema_name, table_name);
        match self.database_table_providers_cache.write() {
            Ok(mut table_provider) => table_provider
                .entry(file_stem.clone())
                .or_insert(Arc::new(MizuTable::new(
                    schema_ref.clone(),
                    ObjectStoreUrl::parse("file://").expect("Parsing object store url"),
                    Arc::from(ParquetSource::new(schema_ref.clone())),
                    ListingTableUrl::parse(&format!("{}/{}.parquet", self.table_path, file_stem))
                        .expect("ParquetTable URL"),
                    self.disk_manager.clone(),
                )))
                .clone(),
            Err(err) => {
                panic!("Error reading table provider cache: {:?}", err);
            }
        }
    }

    // TODO: Refactor exec
    // - Table and Schema creation should be less verbose and in private methods
    async fn exec(&self, stmt: Statement) -> datafusion::error::Result<DataFrame> {
        match self.ctx().state().statement_to_plan(stmt).await? {
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(stmt) => {
                    if self.ctx().table_exist(stmt.name.clone())? {
                        return Err(DataFusionError::Plan(format!(
                            "Table {} already exists",
                            stmt.name.table()
                        )));
                    }
                    let schema = stmt.name.schema().or_else(|| Some(DEFAULT_SCHEMA)).unwrap();
                    let schema_ref = stmt.input.schema();
                    let mut data = JsonValue::new_object();
                    for (_, field) in schema_ref.iter() {
                        let field_str = field.name();
                        let data_type = field.data_type().to_string();
                        data[field_str] = data_type.into();
                    }

                    let table_provider = self.get_table_provider(
                        &schema,
                        stmt.name.table(),
                        schema_ref.as_ref().as_ref(),
                    );

                    let table_source =
                        Arc::new(DefaultTableSource::new(self.catalog_table_provider.clone()));
                    let catalog_input = LogicalPlanBuilder::values(vec![vec![
                        lit(schema),
                        lit(stmt.name.table()),
                        lit(data.to_string()),
                    ]])?
                        .project(vec![
                            col("column1").alias("schema_name"),
                            col("column2").alias("table_name"),
                            col("column3").alias("schema"),
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
                    let _ = collect(stream).await?;
                    self.ctx()
                        .register_table(stmt.name.clone(), table_provider.clone())?;
                    Ok(Self::empty_df_ok(
                        self.ctx().clone().state(),
                        schema_ref.clone(),
                    )?)
                }
                DdlStatement::CreateCatalogSchema(stmt) => {
                    if self.catalog().schema(stmt.schema_name.clone().as_str()).is_some() {
                        return Err(DataFusionError::Plan(format!(
                            "Schema {} already exists",
                            stmt.schema_name.as_str()
                        )));
                    }
                    let parsed = TableReference::from(stmt.schema_name.as_str());
                    let schema_name = parsed.table();
                    let provider = Arc::new(MizuSchemaProvider::new());
                    self.catalog().register_schema(schema_name, provider)?;
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

                    let _ = collect(stream).await?;
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
