use crate::catalog::MizuSchemaProvider;
use datafusion::object_store::path::Path;

enum WALOperation {
    Create,
    Update,
    Delete,
    Checkpoint,
}

struct WALEntry {}

struct MizuWAL {
    path: Path,
    schema_provider: MizuSchemaProvider,
}

impl MizuWAL {
    pub fn new(path: Path, schema_provider: MizuSchemaProvider) -> Self {
        Self {
            path,
            schema_provider,
        }
    }
}
