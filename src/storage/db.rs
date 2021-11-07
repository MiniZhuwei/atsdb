use crate::storage::chunk::MutableChunk;
use hashbrown::HashMap;
use std::sync::Arc;

struct Table {
    mutable: Vec<MutableChunk>,
}

struct DB {
    tables: HashMap<Arc<str>, Table>,
}

impl DB {
    fn new() -> Self {
        return Self {
            tables: HashMap::new(),
        };
    }
}
