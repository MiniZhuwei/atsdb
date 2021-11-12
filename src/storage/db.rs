use crate::storage::chunk::MutableChunk;
use crate::storage::error::DBError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::LogicalPlan;
use datafusion::prelude::ExecutionContext;
use hashbrown::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

struct QueryWorker {
    runtime: Arc<Runtime>,
}

impl QueryWorker {
    fn new(worker_num: usize) -> Self {
        return Self {
            runtime: Arc::new(
                Builder::new_multi_thread()
                    .worker_threads(worker_num)
                    .build()
                    .unwrap(),
            ),
        };
    }

    fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime.spawn(f)
    }
}

struct Table {
    name: Arc<str>,
    mutable: Vec<MutableChunk>,
}

struct DB {
    name: Arc<str>,
    tables: Arc<RwLock<HashMap<Arc<str>, Table>>>,

    worker: QueryWorker,
}

impl DB {
    fn new(name: &str, worker_num: usize) -> Self {
        return Self {
            name: Arc::from(name),
            tables: Arc::new(RwLock::new(HashMap::new())),
            worker: QueryWorker::new(worker_num),
        };
    }

    fn create_table(&mut self, name: &str) {}

    async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, DBError> {
        let ctx = ExecutionContext::new();
        let logical_pan = ctx
            .create_logical_plan(sql)
            .map_err(|err| DBError::InternalError { err })?;
        return match logical_pan {
            LogicalPlan::Filter { .. } => {
                let plan = ctx
                    .optimize(&logical_pan)
                    .map_err(|err| DBError::InternalError { err })?;
                Ok(DataFrameImpl::new(ctx.state, &plan)
                    .collect()
                    .await
                    .map_err(|err| DBError::InternalError { err })?)
            }
            _ => Err(DBError::NoSupportLogicalPlan { plan: logical_pan }),
        };
    }
}
