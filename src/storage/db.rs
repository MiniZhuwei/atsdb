use crate::storage::chunk::{MutableChunk, Scalar, ScalarType};
use crate::storage::error::{DBError, DBWriteError, QueryError};
use async_trait::async_trait;
use chrono::DateTime;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{combine_filters, Expr, LogicalPlan};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::ExecutionContext;
use datafusion::scalar::ScalarValue;
use hashbrown::HashMap;
use std::any::Any;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
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
    mutable_chunks: Vec<Arc<MutableChunk>>,
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
            || timestamp > last_mutable_chunk.unwrap().info.end_at()
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
                chunk.lookup_or_insert(labels).insert(timestamp, &scalars);
                Ok(())
            }
        };
    }

    fn find_on_write_chunk(&mut self, timestamp: SystemTime) -> Option<Arc<MutableChunk>> {
        let start_at = self.mutable_chunks.first().unwrap().info.start_at;
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

    fn push_mutable_chunk(&mut self, chunk: MutableChunk) -> Option<Arc<MutableChunk>> {
        self.mutable_chunks.push(Arc::new(chunk));
        if self.mutable_chunks.len() > self.info.mutable_chunk_num {
            self.archive();
        }
        return self.mutable_chunks.last().cloned();
    }

    fn archive(&mut self) {
        let _archived = self.mutable_chunks.drain(0..1).next().unwrap();
        todo!()
    }

    async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, DBError> {
        let ctx = ExecutionContext::new();
        let logical_pan = ctx
            .create_logical_plan(sql)
            .map_err(|err| DBError::InternalError { err })?;
        let _plan = ctx
            .optimize(&logical_pan)
            .map_err(|err| DBError::InternalError { err })?;
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

#[async_trait]
impl TableProvider for Table {
    fn as_any(&self) -> &dyn Any {
        return self;
    }

    fn schema(&self) -> SchemaRef {
        return Arc::clone(&self.schema);
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let columns = match projection {
            Some(ids) => Arc::new(
                ids.iter()
                    .cloned()
                    .map(|id| self.schema.field(id).clone())
                    .collect(),
            ),
            None => Arc::new(self.schema.fields().clone()),
        };
        let predicate = combine_filters(filters)
            .ok_or_else(|| DataFusionError::Internal(format!("{:?}", QueryError::NoTimeRange)))?;
        let (predicate, start_at, end_at) = self
            .datafusion_range_expr(&predicate)
            .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))?;

        let mut handlers = Vec::new();
        for chunk in self.mutable_chunks.iter().cloned() {
            if start_at > chunk.info.end_at() || end_at < chunk.info.start_at {
                continue;
            }
            let columns = Arc::clone(&columns);
            let predicate = Arc::clone(&predicate);
            handlers.push(tokio::spawn(async move {
                chunk
                    .scan(columns, batch_size, predicate, limit, start_at, end_at)
                    .await
            }));
        }
        let mut partitions = Vec::new();
        for handler in handlers {
            partitions.push(
                handler
                    .await
                    .unwrap()
                    .map_err(|err| DataFusionError::Internal(format!("{:?}", err)))?,
            );
        }
        return Ok(Arc::new(MemoryExec::try_new(
            &[partitions],
            Arc::new(Schema::new(columns.as_ref().clone())),
            None,
        )?));
    }

    fn supports_filter_pushdown(&self, _: &Expr) -> DataFusionResult<TableProviderFilterPushDown> {
        return Ok(TableProviderFilterPushDown::Exact);
    }
}

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
    tables: Arc<RwLock<HashMap<Arc<str>, (Arc<RwLock<Table>>, Arc<mpsc::Sender<InsertRequest>>)>>>,

    query_worker: Worker,
    write_worker: Worker,

    default_series_len: u32,
    default_time_interval: Duration,
    default_mutable_chunk_num: usize,
}

impl DB {
    pub fn new(
        query_worker_num: usize,
        write_worker_num: usize,
        default_series_len: u32,
        default_time_interval: Duration,
        default_mutable_chunk_num: usize,
    ) -> Self {
        return Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            query_worker: Worker::new(query_worker_num),
            write_worker: Worker::new(write_worker_num),

            default_series_len,
            default_time_interval,
            default_mutable_chunk_num,
        };
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, DBError> {
        let ctx = ExecutionContext::new();
        let logical_pan = ctx
            .create_logical_plan(sql)
            .map_err(|err| DBError::InternalError { err })?;
        return match logical_pan {
            LogicalPlan::Filter { .. } => {
                let plan = ctx
                    .optimize(&logical_pan)
                    .map_err(|err| DBError::InternalError { err })?;
                Ok(self
                    .query_worker
                    .spawn(async move { DataFrameImpl::new(ctx.state, &plan).collect().await })
                    .await
                    .unwrap()
                    .map_err(|err| DBError::InternalError { err })?)
            }
            _ => Err(DBError::NoSupportLogicalPlan { plan: logical_pan }),
        };
    }

    pub async fn insert(
        &self,
        table_name: &str,
        timestamp: SystemTime,
        labels: HashMap<String, String>,
        scalars: HashMap<String, Scalar>,
    ) -> Result<(), DBWriteError> {
        let name = Arc::from(table_name);
        let rx = {
            let mut tables = self.tables.write().unwrap();
            let entry = tables.entry(Arc::clone(&name));
            let (_, rx) = entry.or_insert_with(|| {
                self.create_table(
                    name,
                    self.default_series_len,
                    self.default_time_interval,
                    self.default_mutable_chunk_num,
                )
            });
            Arc::clone(rx)
        };
        let (ret, recv) = oneshot::channel::<Result<(), DBWriteError>>();
        rx.send(InsertRequest {
            timestamp,
            labels,
            scalars,
            ret,
        })
        .await
        .map_err(|err| DBWriteError::DBInternalError {
            desc: format!("{}", err),
        })?;
        return recv.await.unwrap();
    }

    fn create_table(
        &self,
        table_name: Arc<str>,
        series_len: u32,
        time_interval: Duration,
        mutable_chunk_num: usize,
    ) -> (Arc<RwLock<Table>>, Arc<mpsc::Sender<InsertRequest>>) {
        let table = Arc::new(RwLock::new(Table::new(
            table_name,
            series_len,
            time_interval,
            mutable_chunk_num,
        )));
        let (tx, mut rx) = mpsc::channel::<InsertRequest>(1);
        let table_worker = Arc::clone(&table);
        self.write_worker.spawn(async move {
            while let Some(request) = rx.recv().await {
                request
                    .ret
                    .send(table_worker.write().unwrap().insert(
                        request.timestamp,
                        request.labels,
                        request.scalars,
                    ))
                    .unwrap_or_else(|err| panic!("{:?}", err));
            }
        });
        return (table, Arc::new(tx));
    }
}

struct InsertRequest {
    timestamp: SystemTime,
    labels: HashMap<String, String>,
    scalars: HashMap<String, Scalar>,
    ret: oneshot::Sender<Result<(), DBWriteError>>,
}

#[cfg(test)]
mod tests {
    use crate::storage::chunk::Scalar;
    use crate::storage::db::Table;
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
        ctx.register_table(Arc::clone(&table.name).as_ref(), Arc::new(table))
            .unwrap();
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
