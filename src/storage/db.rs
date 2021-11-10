use crate::storage::chunk::MutableChunk;
use hashbrown::HashMap;
use std::sync::{Arc, RwLock};

struct Table {
    name: Arc<str>,
    mutable: Vec<MutableChunk>,
}

struct DB {
    name: Arc<str>,
    tables: Arc<RwLock<HashMap<Arc<str>, Table>>>,
}

impl DB {
    fn new(name: &str) -> Self {
        return Self {
            name: Arc::from(name),
            tables: Arc::new(RwLock::new(HashMap::new())),
        };
    }

    fn create_table(&mut self) {}
}
