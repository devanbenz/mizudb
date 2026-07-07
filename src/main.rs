mod catalog;
mod data_sink;
mod object_store;
mod wal;

use crate::catalog::{MizuCatalog, MizuSchemaProvider};
use crate::object_store::{MizuObjectStore, MizuObjectStoreRegistry};
use datafusion::catalog::CatalogProvider;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DdlStatement, DmlStatement, EmptyRelation, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::TableReference;
use std::io::Write;
use std::sync::Arc;
use url::Url;

struct MizuDB {
    catalog: Arc<dyn CatalogProvider>,
    session_ctx: SessionContext,
}

impl MizuDB {
    async fn new(catalog: Arc<dyn CatalogProvider>) -> datafusion::error::Result<Self> {
        let object_store = Arc::new(MizuObjectStore::new("/tmp/datafusion_tmp").await.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?);
        let object_store_registry = Arc::new(MizuObjectStoreRegistry::with_default_store(
            Arc::new(object_store),
            Url::try_from("file:///tmp/datafusion_tmp/").unwrap(),
            "".to_string(),
        ));
        let rt = RuntimeEnvBuilder::default()
            .with_object_store_registry(object_store_registry)
            .build()?;

        let session_config = SessionConfig::new();
        let session_ctx = SessionContext::new_with_config_rt(session_config, Arc::new(rt));
        session_ctx.register_catalog("mizu", catalog.clone());

        Ok(Self {
            catalog,
            session_ctx,
        })
    }

    fn ctx(&self) -> &SessionContext {
        &self.session_ctx
    }

    fn catalog(&self) -> &Arc<dyn CatalogProvider> {
        &self.catalog
    }

    async fn exec(&self, stmt: Statement) -> datafusion::error::Result<DataFrame> {
        match self.ctx().state().statement_to_plan(stmt).await? {
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateMemoryTable(stmt) => {
                    let schema =
                        stmt.name.schema().or_else(|| Some("default")).unwrap();
                    let schema_ref = stmt.input.schema();
                    let base_path = "file:///tmp/datafusion_tmp/";
                    let listing_table_provider = ListingTable::try_new(
                        ListingTableConfig::new(ListingTableUrl::parse(&base_path)?)
                            .with_listing_options(ListingOptions::new(Arc::new(
                                ParquetFormat::new(),
                            )))
                            .with_schema(schema_ref.inner().clone()),
                    )?;

                    self.ctx().register_table(
                        stmt.name.clone(),
                        Arc::new(listing_table_provider.clone()),
                    )?;
                    Ok(Self::empty_df_ok(self.ctx().clone().state(), schema_ref.clone())?)
                }
                DdlStatement::CreateCatalogSchema(stmt) => {
                    let parsed = TableReference::from(stmt.schema_name.as_str());
                    let schema_name = parsed.table();
                    let provider = Arc::new(MizuSchemaProvider::new());
                    self.catalog().register_schema(schema_name, provider)?;
                    Ok(Self::empty_df_ok(self.ctx().clone().state(), stmt.schema.clone())?)
                }
                _ => unimplemented!(),
            },
            LogicalPlan::Dml(dml) => match dml {
                DmlStatement { table_name, target, op, input, output_schema } => {
                    let logical_input = input.as_ref().clone();
                    let logical_plan = LogicalPlanBuilder::insert_into(logical_input, table_name, target, InsertOp::Append)?.build()?;
                    let physical_plan = self.ctx().state().create_physical_plan(&logical_plan).await?;
                    let stream = execute_stream(physical_plan.clone(), self.ctx().task_ctx().clone())?;
                    let streams = collect(stream).await?;
                    for stream in streams {
                        println!("{:#?}", stream);
                    }
                    Ok(Self::empty_df_ok(self.ctx().clone().state(), output_schema.clone())?)
                }
            },
            plan => {
                Ok(self.ctx().execute_logical_plan(plan).await?)
            }
        }
    }

    fn empty_df_ok(session_state: SessionState, schema: Arc<DFSchema>) -> datafusion::error::Result<DataFrame> {
        Ok(DataFrame::new(session_state, LogicalPlan::EmptyRelation(EmptyRelation { produce_one_row: false, schema: schema.clone() })))
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

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let catalog = MizuCatalog::new();
    let db = MizuDB::new(Arc::new(catalog)).await?;
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
