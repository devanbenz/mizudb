mod catalog;
mod data_sink;
mod object_store;
mod wal;

use crate::catalog::{MizuCatalog, MizuSchemaProvider};
use crate::object_store::MizuObjectStoreRegistry;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{DdlStatement, DmlStatement, LogicalPlan};
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::parser::DFParser;
use std::io::Write;
use std::sync::Arc;
use url::Url;

struct MizuDB {
    catalog: Arc<dyn CatalogProvider>,
    session_ctx: SessionContext,
}

impl MizuDB {
    fn new(catalog: Arc<dyn CatalogProvider>) -> datafusion::error::Result<Self> {
        let object_store_registry = Arc::new(MizuObjectStoreRegistry::with_default_store(
            Arc::new(InMemory::new()),
            Url::try_from("file://").unwrap(),
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

    async fn exec(&self, sql: &str) -> datafusion::error::Result<()> {
        match DFParser::parse_sql(sql) {
            Ok(stmts) => {
                for stmt in stmts {
                    match self.ctx().state().statement_to_plan(stmt).await? {
                        LogicalPlan::Ddl(ddl) => match ddl {
                            DdlStatement::CreateExternalTable(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::CreateMemoryTable(stmt) => {
                                let schema =
                                    stmt.name.schema().or_else(|| Some("default")).unwrap();
                                let schema_ref = stmt.input.schema();
                                let listing_table_provider = ListingTable::try_new(
                                    ListingTableConfig::new(ListingTableUrl::parse("file://")?)
                                        .with_listing_options(ListingOptions::new(Arc::new(
                                            ParquetFormat::new(),
                                        )))
                                        .with_schema(schema_ref.inner().clone()),
                                )?;

                                self.ctx().register_table(
                                    stmt.name.clone(),
                                    Arc::new(listing_table_provider.clone()),
                                )?;
                                for cat in self.ctx().catalog_names() {
                                    let c = self.ctx().catalog(&cat).unwrap();
                                    for sch in c.schema_names() {
                                        let s = c.schema(&sch).unwrap();
                                        println!("{cat}.{sch}: {:?}", s.table_names());
                                    }
                                }
                            }
                            DdlStatement::CreateView(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::CreateCatalogSchema(stmt) => {
                                println!("{:#?}", stmt);
                                let parsed = TableReference::from(stmt.schema_name.as_str());
                                let schema_name = parsed.table();
                                let provider = Arc::new(MizuSchemaProvider::new());
                                self.catalog().register_schema(schema_name, provider)?;
                            }
                            DdlStatement::CreateCatalog(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::CreateIndex(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::DropTable(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::DropView(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::DropCatalogSchema(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::CreateFunction(stmt) => {
                                println!("{:#?}", stmt);
                            }
                            DdlStatement::DropFunction(stmt) => {
                                println!("{:#?}", stmt);
                            }
                        },
                        LogicalPlan::Dml(dml) => match dml {
                            DmlStatement { .. } => {
                                println!("{:#?}", dml);
                            }
                        },
                        plan => {
                            self.ctx().execute_logical_plan(plan).await?;
                        }
                    }
                }
            }
            Err(err) => {
                println!("{:#?}", err);
            }
        };

        Ok(())
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
    let db = MizuDB::new(Arc::new(catalog))?;
    loop {
        let input = prompt("> ");
        if input == "exit" {
            break;
        }
        db.exec(input.as_str()).await?;
    }

    Ok(())
}
