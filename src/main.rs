mod catalog;
mod data_sink;
mod object_store;
mod table;
mod wal;

use crate::catalog::{MizuCatalog, MizuSchemaProvider};
use crate::object_store::{MizuObjectStore, MizuObjectStoreRegistry};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::CatalogProvider;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    DdlStatement, DmlStatement, EmptyRelation, LogicalPlan, LogicalPlanBuilder, TableSource,
    WriteOp,
};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::sql::TableReference;
use datafusion::sql::parser::{DFParser, Statement};
use std::fs::exists;
use std::io::Write;
use std::sync::Arc;
use url::Url;

const DEFAULT_SCHEMA: &str = "public";
const DEFAULT_CATALOG: &str = "mizudb";

struct MizuDB {
    catalog: Arc<dyn CatalogProvider>,
    catalog_table_provider: Arc<dyn TableProvider>,
    session_ctx: SessionContext,
    path: String,
    table_path: String,
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
            Arc::new(object_store),
            Url::try_from(table_path.as_str()).unwrap(),
        ));

        let catalog_schema = SchemaRef::new(Schema::new(vec![
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
        ]));
        let catalog_table_provider = Arc::new(ListingTable::try_new(
            ListingTableConfig::new(ListingTableUrl::parse(&format!(
                "{}/catalog.parquet",
                table_path
            ))?)
            .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::new())))
            .with_schema(catalog_schema.clone()),
        )?);

        let rt = RuntimeEnvBuilder::default()
            .with_object_store_registry(object_store_registry)
            .build()?;

        let session_config =
            SessionConfig::new().with_default_catalog_and_schema(DEFAULT_CATALOG, DEFAULT_SCHEMA);
        let session_ctx = SessionContext::new_with_config_rt(session_config, Arc::new(rt));
        session_ctx.register_catalog(DEFAULT_CATALOG, catalog.clone());
        catalog.register_schema(DEFAULT_SCHEMA, Arc::new(MizuSchemaProvider::new()))?;
        let table_ref = TableReference::full(DEFAULT_CATALOG, DEFAULT_SCHEMA, "mizudb_store");
        session_ctx.register_table(table_ref.clone(), catalog_table_provider.clone())?;

        // TODO: Check if db file exists, if not create it, if it does load it in to the catalog.
        if exists(path.as_str())? {
            // Self::load_db(catalog).await
            Ok(Self {
                catalog,
                catalog_table_provider,
                session_ctx,
                path: db_path,
                table_path,
            })
        } else {
            Ok(Self {
                catalog,
                catalog_table_provider,
                session_ctx,
                path: db_path,
                table_path,
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
            catalog,
            catalog_table_provider,
            session_ctx: SessionContext::new(),
            path: "".to_string(),
            table_path: "".to_string(),
        })
    }

    // TODO: Refactor exec
    // - Table and Schema creation should be less verbose and in private methods
    async fn exec(&self, stmt: Statement) -> datafusion::error::Result<DataFrame> {
        match self.ctx().state().statement_to_plan(stmt).await? {
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(stmt) => {
                    let schema = stmt.name.schema().or_else(|| Some(DEFAULT_SCHEMA)).unwrap();

                    println!("Schema {:?}", schema);
                    println!("Schemas {:?}", self.catalog.clone().schema_names());
                    let schema_ref = stmt.input.schema();
                    let listing_table_provider = ListingTable::try_new(
                        ListingTableConfig::new(ListingTableUrl::parse(&format!(
                            "{}/{}.parquet",
                            self.table_path,
                            stmt.name.table()
                        ))?)
                        .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::new())))
                        .with_schema(schema_ref.inner().clone()),
                    )?;

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
                    self.ctx().register_table(
                        stmt.name.clone(),
                        Arc::new(listing_table_provider.clone()),
                    )?;
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
        let stmt = DFParser::parse_sql(&input)?;
        for s in stmt {
            let df = db.exec(s).await?;
            df.show().await?;
        }
    }

    Ok(())
}
