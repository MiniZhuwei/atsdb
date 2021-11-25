use crate::storage::chunk::{MutableChunk, Scalar, ScalarType};
use crate::storage::error::{DBError, DBWriteError, QueryError};
use crate::storage::util::thread::CoreBoundWorkers;
use chrono::DateTime;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::prelude::ExecutionContext;
use datafusion::scalar::ScalarValue;
use hashbrown::HashMap;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::future::Future;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
struct Worker {
    runtime: Arc<Runtime>,
}

impl Worker {
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

#[derive(Debug)]
struct Table {
    name: Arc<str>,
    mutable_chunks: Vec<Rc<RefCell<MutableChunk>>>,
    schema: Arc<Schema>,

    info: TableInfo,
}

impl Table {
    fn new(
        name: Arc<str>,
        series_len: u32,
        time_interval: Duration,
        mutable_chunk_num: usize,
    ) -> Self {
        let mutable_chunks = Vec::new();
        Self {
            name,
            mutable_chunks,
            schema: Arc::new(Schema::new(Vec::new())),
            info: TableInfo {
                series_len,
                time_interval,
                mutable_chunk_num,
            },
        }
    }

    fn insert(
        &mut self,
        timestamp: SystemTime,
        labels: HashMap<String, String>,
        scalars: HashMap<String, Scalar>,
    ) -> Result<(), DBWriteError> {
        let last_mutable_chunk = self.mutable_chunks.last();
        let writing = if last_mutable_chunk.is_none()
            || timestamp > last_mutable_chunk.unwrap().borrow().info.end_at()
        {
            let num = timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .div_duration_f64(self.info.time_interval * self.info.series_len);
            let start_at = UNIX_EPOCH + self.info.series_len * self.info.time_interval * num as u32;
            let mut chunk = self.new_mutable_chunk(start_at);
            chunk.set_schema(
                labels.keys().map(String::as_ref).collect(),
                scalars
                    .iter()
                    .map(|(key, value)| match value {
                        ScalarType::Int(_) => ScalarType::Int(key.as_ref()),
                        ScalarType::Float(_) => ScalarType::Float(key.as_ref()),
                    })
                    .collect(),
            );
            self.schema = Arc::new(
                Schema::try_merge(vec![
                    self.schema.as_ref().clone(),
                    chunk.schema().as_ref().clone(),
                ])
                .map_err(|err| DBWriteError::UpdateSchemaError { err })?,
            );
            self.push_mutable_chunk(chunk)
        } else {
            self.find_on_write_chunk(timestamp)
        };
        return match writing {
            None => Err(DBWriteError::ChunkArchived { timestamp }),
            Some(chunk) => {
                chunk
                    .deref()
                    .borrow_mut()
                    .lookup_or_insert(labels)
                    .insert(timestamp, &scalars);
                Ok(())
            }
        };
    }

    fn find_on_write_chunk(&mut self, timestamp: SystemTime) -> Option<Rc<RefCell<MutableChunk>>> {
        let start_at = self.mutable_chunks.first().unwrap().borrow().info.start_at;
        if timestamp < start_at {
            return None;
        }

        let chunk_duration = self.info.get_chunk_duration();
        let offset = timestamp
            .duration_since(start_at)
            .unwrap()
            .div_duration_f64(chunk_duration) as usize;
        return self.mutable_chunks.get(offset).cloned();
    }

    fn new_mutable_chunk(&self, start_at: SystemTime) -> MutableChunk {
        return MutableChunk::new(start_at, self.info.time_interval, self.info.series_len);
    }

    fn push_mutable_chunk(&mut self, chunk: MutableChunk) -> Option<Rc<RefCell<MutableChunk>>> {
        self.mutable_chunks.push(Rc::new(RefCell::new(chunk)));
        if self.mutable_chunks.len() > self.info.mutable_chunk_num {
            self.archive();
        }
        return self.mutable_chunks.last().cloned();
    }

    fn archive(&mut self) {
        let _archived = self.mutable_chunks.drain(0..1).next().unwrap();
        todo!()
    }

    fn datafusion_get_timestamp(expr: &Expr) -> Result<SystemTime, QueryError> {
        return if let Expr::Literal(ScalarValue::Utf8(Some(v))) = expr {
            DateTime::parse_from_rfc3339(v)
                .map(SystemTime::from)
                .map_err(|v| QueryError::WrongTimeRange {
                    desc: v.to_string(),
                })
        } else {
            Err(QueryError::WrongTimeRange {
                desc: expr.to_string(),
            })
        };
    }

    fn datafusion_range_expr(
        &self,
        predicate: &Expr,
    ) -> Result<(Arc<Option<Expr>>, SystemTime, SystemTime), QueryError> {
        return if let Expr::ScalarUDF { fun, args } = predicate {
            if fun.name != "range" || args.len() < 2 {
                return Err(QueryError::NoTimeRange);
            }
            Ok((
                Arc::new(args.get(2).cloned()),
                Self::datafusion_get_timestamp(args.get(0).unwrap())?,
                Self::datafusion_get_timestamp(args.get(1).unwrap())?,
            ))
        } else {
            Err(QueryError::NoTimeRange)
        };
    }
}

// #[async_trait]
// impl TableProvider for Table {
//     fn as_any(&self) -> &dyn Any {
//         return self;
//     }
//
//     fn schema(&self) -> SchemaRef {
//         return Arc::clone(&self.schema);
//     }
//
//     async fn scan(
//         &self,
//         projection: &Option<Vec<usize>>,
//         batch_size: usize,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
//         let columns = match projection {
//             Some(ids) => Arc::new(
//                 ids.iter()
//                     .cloned()
//                     .map(|id| self.schema.field(id).clone())
//                     .collect(),
//             ),
//             None => Arc::new(self.schema.fields().clone()),
//         };
//         let predicate = combine_filters(filters)
//             .ok_or_else(|| DataFusionError::Internal(format!("{:?}", QueryError::NoTimeRange)))?;
//         let (predicate, start_at, end_at) = self
//             .datafusion_range_expr(&predicate)
//             .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))?;
//
//         let mut handlers = Vec::new();
//         for chunk in self.mutable_chunks.iter().cloned() {
//             if start_at > chunk.info.end_at() || end_at < chunk.info.start_at {
//                 continue;
//             }
//             let columns = Arc::clone(&columns);
//             let predicate = Arc::clone(&predicate);
//             handlers.push(tokio::spawn(async move {
//                 chunk
//                     .scan(columns, batch_size, predicate, limit, start_at, end_at)
//                     .await
//             }));
//         }
//         let mut partitions = Vec::new();
//         for handler in handlers {
//             partitions.push(
//                 handler
//                     .await
//                     .unwrap()
//                     .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?,
//             );
//         }
//         return Ok(Arc::new(MemoryExec::try_new(
//             &[partitions],
//             Arc::new(Schema::new(columns.as_ref().clone())),
//             None,
//         )?));
//     }
//
//     fn supports_filter_pushdown(&self, _: &Expr) -> DataFusionResult<TableProviderFilterPushDown> {
//         return Ok(TableProviderFilterPushDown::Exact);
//     }
// }

#[derive(Debug)]
struct TableInfo {
    series_len: u32,
    time_interval: Duration,
    mutable_chunk_num: usize,
}

impl TableInfo {
    fn get_chunk_duration(&self) -> Duration {
        return self.time_interval * self.series_len;
    }
}

#[derive(Debug)]
pub struct DB {
    tables: RefCell<HashMap<Arc<str>, Table>>,
    default_series_len: u32,
    default_time_interval: Duration,
    default_mutable_chunk_num: usize,
}

impl DB {
    fn new(
        default_series_len: u32,
        default_time_interval: Duration,
        default_mutable_chunk_num: usize,
    ) -> Self {
        return Self {
            tables: RefCell::new(HashMap::new()),

            default_series_len,
            default_time_interval,
            default_mutable_chunk_num,
        };
    }

    async fn sql_query(&self, sql: &str) -> Result<Vec<RecordBatch>, DBError> {
        let ctx = ExecutionContext::new();
        let logical_pan = ctx
            .create_logical_plan(sql)
            .map_err(|err| DBError::InternalError { err })?;
        return match logical_pan {
            LogicalPlan::Filter { .. } => {
                let plan = ctx
                    .optimize(&logical_pan)
                    .map_err(|err| DBError::InternalError { err })?;
                Ok(task::spawn_local(async move {
                    DataFrameImpl::new(ctx.state, &plan).collect().await
                })
                .await
                .unwrap()
                .map_err(|err| DBError::InternalError { err })?)
            }
            _ => Err(DBError::NoSupportLogicalPlan { plan: logical_pan }),
        };
    }

    fn insert(
        &self,
        table_name: Arc<str>,
        timestamp: SystemTime,
        labels: HashMap<String, String>,
        scalars: HashMap<String, Scalar>,
    ) -> Result<(), DBWriteError> {
        let mut table = self.tables.borrow_mut();
        let entry = table.entry(Arc::clone(&table_name));
        let table = entry.or_insert_with(|| {
            self.create_table(
                table_name,
                self.default_series_len,
                self.default_time_interval,
                self.default_mutable_chunk_num,
            )
        });
        return table.insert(timestamp, labels, scalars).map_err(|err| {
            DBWriteError::DBInternalError {
                desc: format!("{:?}", err),
            }
        });
    }

    fn create_table(
        &self,
        table_name: Arc<str>,
        series_len: u32,
        time_interval: Duration,
        mutable_chunk_num: usize,
    ) -> Table {
        return Table::new(table_name, series_len, time_interval, mutable_chunk_num);
    }
}

#[derive(Debug)]
struct InsertRequest {
    table_name: Arc<str>,
    timestamp: SystemTime,
    labels: HashMap<String, String>,
    scalars: HashMap<String, Scalar>,
    ret: oneshot::Sender<Result<(), DBWriteError>>,
}

#[derive(Debug)]
pub struct ShardedDB {
    labels: Arc<RwLock<Vec<String>>>,
    senders: Vec<Sender<InsertRequest>>,
    // dones: Vec<Receiver<Result<(), thread::Error>>>,
}

impl ShardedDB {
    pub async fn new(
        workers: &CoreBoundWorkers,
        default_series_len: u32,
        default_time_interval: Duration,
        default_mutable_chunk_num: usize,
    ) -> Self {
        let mut senders = Vec::new();
        // let mut dones = Vec::new();
        for worker in workers.iter() {
            let (insert_req, mut insert_recv) = mpsc::channel::<InsertRequest>(1);
            let res = worker
                .spawn(move || {
                    Box::pin(async move {
                        let db = DB::new(
                            default_series_len,
                            default_time_interval,
                            default_mutable_chunk_num,
                        );
                        while let Some(req) = insert_recv.recv().await {
                            let ret =
                                db.insert(req.table_name, req.timestamp, req.labels, req.scalars);
                            req.ret.send(ret).unwrap();
                        }
                    })
                })
                .await;
            if res.is_err() {
                panic!("test");
            }
            // dones.push(done);
            senders.push(insert_req);
        }
        return Self {
            senders,
            // dones,
            labels: Arc::new(RwLock::new(Vec::new())),
        };
    }

    pub async fn insert(
        &self,
        table_name: Arc<str>,
        timestamp: SystemTime,
        labels: HashMap<String, String>,
        scalars: HashMap<String, Scalar>,
    ) -> Result<(), DBWriteError> {
        let mut labels_clone = labels.clone();
        let mut label_values = {
            let schema = self.labels.read().unwrap();
            let mut label_values = Vec::with_capacity(schema.len());
            for label_key in schema.iter() {
                match labels_clone.remove_entry(label_key) {
                    Some((_, value)) => label_values.push(value),
                    None => label_values.push(String::from("value")),
                }
            }
            label_values
        };
        if !labels_clone.is_empty() {
            let mut labels_clone = labels.clone();
            let mut schema = self.labels.write().unwrap();
            label_values = Vec::with_capacity(schema.len());
            for label_key in schema.iter() {
                match labels_clone.remove_entry(label_key) {
                    Some((_, value)) => label_values.push(value),
                    None => label_values.push(String::from("value")),
                }
            }
            if !labels_clone.is_empty() {
                for (key, value) in labels_clone.drain() {
                    schema.push(key);
                    label_values.push(value);
                }
            }
        }
        let hash = seahash::hash(
            label_values
                .iter()
                .flat_map(|s| s.chars())
                .collect::<String>()
                .as_ref(),
        );
        let shard = hash as usize % self.senders.len();
        let (ret, ret_recv) = oneshot::channel();
        self.senders
            .get(shard)
            .unwrap()
            .send(InsertRequest {
                table_name,
                timestamp,
                labels,
                scalars,
                ret,
            })
            .await
            .unwrap();
        return ret_recv.await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::chunk::Scalar;
    use crate::storage::db::{Table, DB};
    use crate::storage::query::{make_range_udf, make_time_udf};
    use chrono::{DateTime, Utc};
    use datafusion::prelude::ExecutionContext;
    use hashbrown::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn test_query() {
        let now = SystemTime::now();
        let mut table = Table::new(Arc::from("test"), 128, Duration::SECOND, 1);
        let labels = [
            ("hefei".to_string(), "miemie".to_string()),
            ("yueyang".to_string(), "momo".to_string()),
        ]
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
        let scalars = [
            ("v1".to_string(), Scalar::Int(1)),
            ("v2".to_string(), Scalar::Float(0.1)),
        ]
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
        table.insert(now, labels, scalars).unwrap();
        println!("{:?}", table.mutable_chunks);

        let mut ctx = ExecutionContext::new();
        // ctx.register_table(Arc::clone(&table.name).as_ref(), Arc::new(table))
        //     .unwrap();
        ctx.register_udf(make_range_udf());
        ctx.register_udf(make_time_udf());
        let sql_results = ctx
            .sql(&*format!(
                "select * from test where range(datetime('{}'), datetime('{}'), hefei = 'miemie' and yueyang = 'momo')",
                DateTime::<Utc>::from(now).to_rfc3339(),
                DateTime::<Utc>::from(now + Duration::SECOND).to_rfc3339())
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        println!("{:?}", sql_results);
    }
}
