use crate::storage::error::QueryError;
use crate::storage::query::Matcher;
use crate::storage::util::dictionary::StringDictionary;
use datafusion::arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, ListBuilder, StringArray as ArrowStringArray,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_plan::{combine_filters, Expr, Operator as ArrowOperator};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use hashbrown::{HashMap, HashSet};
use regex::Regex;
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Row<'a> {
    chunk: &'a MutableChunk,
    id: usize,
}

impl<'a> Row<'a> {
    fn new(chunk: &'a MutableChunk, id: usize) -> Self {
        return Self { id, chunk };
    }

    fn insert(&self, timestamp: Instant, scalars: HashMap<&str, Scalar>) {
        let mut columns = self.chunk.columns.write().unwrap();
        for (name, column) in columns.scalars.iter_mut() {
            let series = column.get(self.id).unwrap();
            let scalar = scalars.get(name.as_ref()).cloned();
            let index = (timestamp - self.chunk.info.start_at)
                .div_duration_f64(self.chunk.info.time_interval) as u32;
            match series {
                ScalarType::Int(series) => series
                    .write()
                    .unwrap()
                    .insert(index, scalar.map(|s| s.into())),
                ScalarType::Float(series) => series
                    .write()
                    .unwrap()
                    .insert(index, scalar.map(|s| s.into())),
            }
        }
    }
}

#[derive(Debug)]
struct Series<G> {
    data: Vec<Option<G>>,
}

impl<G: 'static + Default + Clone + Copy> Series<G> {
    fn new(len: u32) -> Self {
        return Self {
            data: vec![None; len as usize],
        };
    }

    fn insert(&mut self, index: u32, scalar: Option<G>) {
        self.data.insert(index as usize, scalar);
    }

    fn iter(&self) -> impl Iterator<Item = Option<G>> + '_ {
        return self.data.iter().cloned();
    }
}

#[derive(Debug, Clone, Copy)]
enum ScalarType<T, G> {
    Int(T),
    Float(G),
}

type Scalar = ScalarType<i64, f64>;

impl From<Scalar> for f64 {
    #[inline]
    fn from(scalar: Scalar) -> Self {
        return match scalar {
            Scalar::Int(v) => v as Self,
            Scalar::Float(v) => v,
        };
    }
}

impl From<Scalar> for i64 {
    #[inline]
    fn from(scalar: Scalar) -> Self {
        return match scalar {
            Scalar::Float(v) => v as Self,
            Scalar::Int(v) => v,
        };
    }
}

type LockedSeries<G> = RwLock<Series<G>>;

#[derive(Debug)]
struct ScalarColumn {
    series_len: u32,
    data: ScalarType<Vec<LockedSeries<i64>>, Vec<LockedSeries<f64>>>,
}

impl ScalarColumn {
    fn new(column_type: ScalarType<(), ()>, series_len: u32) -> Self {
        return match column_type {
            ScalarType::Int(_) => Self {
                series_len,
                data: ScalarType::Int(Vec::new()),
            },
            ScalarType::Float(_) => Self {
                series_len,
                data: ScalarType::Float(Vec::new()),
            },
        };
    }

    fn resize(&mut self, to: u32) {
        match &mut self.data {
            ScalarType::Int(column) => {
                column.resize_with(to as usize, || RwLock::new(Series::new(self.series_len)))
            }
            ScalarType::Float(column) => {
                column.resize_with(to as usize, || RwLock::new(Series::new(self.series_len)))
            }
        };
    }

    fn push_zero(&mut self) {
        match &mut self.data {
            ScalarType::Int(column) => column.push(RwLock::new(Series::new(self.series_len))),
            ScalarType::Float(column) => column.push(RwLock::new(Series::new(self.series_len))),
        };
    }

    fn get(&self, offset: usize) -> Option<ScalarType<&LockedSeries<i64>, &LockedSeries<f64>>> {
        return match &self.data {
            ScalarType::Int(data) => data.get(offset).map(ScalarType::Int),
            ScalarType::Float(data) => data.get(offset).map(ScalarType::Float),
        };
    }
}

#[derive(Debug, Default)]
struct LabelColumn {
    data: Vec<usize>,
    values: StringDictionary,
}

impl LabelColumn {
    fn new() -> Self {
        return Self {
            data: Vec::new(),
            values: StringDictionary::new(),
        };
    }

    fn resize(&mut self, to: u32) {
        self.data.resize(to as usize, 0);
    }

    fn push(&mut self, s: &str) {
        let id = self.values.lookup_or_insert(s);
        self.data.push(id);
    }

    fn push_zero(&mut self) {
        self.data.push(0);
    }

    fn get(&self, id: usize) -> Option<&str> {
        return self.values.get(*self.data.get(id)?);
    }
}

struct Filter<'a> {
    name: &'a str,
    matcher: Matcher<'a>,
}

impl<'a> Filter<'a> {
    fn filter(
        &self,
        column: &'a LabelColumn,
        pre_filtered: Option<HashSet<usize>>,
    ) -> Option<Vec<usize>> {
        let filter_id: Vec<_> = match pre_filtered {
            Some(pre_filtered) => pre_filtered.into_iter().collect(),
            None => (0..column.data.len()).into_iter().collect(),
        };
        let iter = filter_id
            .into_iter()
            .map(|id| *column.data.get(id).unwrap());

        let match_func: Box<dyn Fn(&(usize, usize)) -> bool> = match self.matcher {
            Matcher::LiteralEqual(v) => {
                let sid = match v {
                    Some(s) => column.values.lookup(s)?,
                    None => 0,
                };
                Box::new(move |(_, record): &(usize, usize)| *record == sid)
            }
            Matcher::LiteralNotEqual(v) => {
                let sid = match v {
                    Some(s) => column.values.lookup(s)?,
                    None => 0,
                };
                Box::new(move |(_, record): &(usize, usize)| *record != sid)
            }
            Matcher::RegexMatch(v) => {
                let regex = Regex::new(v).ok()?;
                Box::new(move |(_, record): &(usize, usize)| {
                    regex.is_match(column.values.get(*record).unwrap())
                })
            }
            Matcher::RegexNotMatch(v) => {
                let regex = Regex::new(v).ok()?;
                Box::new(move |(_, record): &(usize, usize)| {
                    !regex.is_match(column.values.get(*record).unwrap())
                })
            }
        };
        return Some(
            iter.enumerate()
                .filter(match_func)
                .map(|(id, _)| id)
                .collect(),
        );
    }
}

#[derive(Debug, Default)]
struct Columns {
    labels: HashMap<Arc<str>, LabelColumn>,
    scalars: HashMap<Arc<str>, ScalarColumn>,
    arrows: Vec<ArrowField>,
}

impl Columns {
    fn new() -> Self {
        return Default::default();
    }

    fn filter_label(&self, filter: Filter, pre_filtered: Option<HashSet<usize>>) -> HashSet<usize> {
        let mut pre_filtered = pre_filtered;
        if let Some(records) = self.labels.get(filter.name).and_then(|column| {
            filter.filter(
                column,
                pre_filtered.as_ref().map(|v| v.iter().cloned().collect()),
            )
        }) {
            let set = records.iter().cloned().collect::<HashSet<_>>();
            pre_filtered = match pre_filtered {
                None => Some(set),
                Some(inter) => Some(inter.intersection(&set).cloned().collect()),
            };
        }
        return pre_filtered.unwrap_or_default();
    }
}

#[derive(Debug)]
pub struct MutableChunk {
    info: ChunkInfo,
    stat: ChunkStat,

    columns: Arc<RwLock<Columns>>,
}

impl<'a> MutableChunk {
    fn new(start_at: Instant, time_interval: Duration, series_len: u32) -> Self {
        return Self {
            info: ChunkInfo::new(start_at, time_interval, series_len),
            stat: ChunkStat::new(),
            columns: Arc::new(RwLock::new(Columns::new())),
        };
    }

    fn set_schema(&mut self, labels: Vec<&str>, scalars: Vec<ScalarType<&str, &str>>) {
        let mut columns = self.on_write();

        for name in labels.into_iter() {
            columns.labels.insert(Arc::from(name), LabelColumn::new());
            columns
                .arrows
                .push(ArrowField::new(name, ArrowDataType::Utf8, true));
        }
        for scalar in scalars.into_iter() {
            let (name, scalar_type, arrow_type) = match scalar {
                ScalarType::Int(name) => (name, ScalarType::Int(()), ArrowDataType::Int64),
                ScalarType::Float(name) => (name, ScalarType::Float(()), ArrowDataType::Float64),
            };
            columns.scalars.insert(
                Arc::from(name),
                ScalarColumn::new(scalar_type, self.info.series_len),
            );
            columns.arrows.push(ArrowField::new(
                name,
                ArrowDataType::List(Box::new(ArrowField::new("item", arrow_type, true))),
                false,
            ));
        }
    }

    fn create_record(
        &'a self,
        mut lock: RwLockWriteGuard<'a, Columns>,
        labels: HashMap<&str, &str>,
    ) -> Row<'a> {
        for (name, column) in lock.labels.iter_mut() {
            let label_value = labels.get(name.as_ref());
            match label_value {
                Some(v) => column.push(v),
                None => column.push_zero(),
            }
        }

        for column in lock.scalars.values_mut() {
            column.push_zero();
        }

        return Row::new(self, self.stat.add_record_num(1));
    }

    fn filter(&self, filter: Filter, pre_filtered: Option<HashSet<usize>>) -> HashSet<usize> {
        let columns = self.on_read();
        return columns.filter_label(filter, pre_filtered);
    }

    fn lookup_or_insert(&'a self, labels: HashMap<&str, &str>) -> Row<'a> {
        let mut pre_filtered = None;
        let lock = self.columns.write().unwrap();
        for column_name in lock.labels.keys() {
            pre_filtered = Some(lock.filter_label(
                Filter {
                    name: column_name,
                    matcher: Matcher::LiteralEqual(labels.get(column_name.as_ref()).cloned()),
                },
                pre_filtered,
            ));
        }
        return match pre_filtered {
            Some(set) => set
                .iter()
                .cloned()
                .next()
                .map(|id| Row::new(self, id))
                .unwrap_or_else(|| self.create_record(lock, labels)),
            None => self.create_record(lock, labels),
        };
    }

    fn to_arrow(
        &self,
        projection: Vec<usize>,
        ids: &HashSet<usize>,
    ) -> Result<RecordBatch, QueryError> {
        let mut fields = Vec::new();
        let mut arrow_arrays = Vec::<ArrayRef>::new();
        let columns = self.on_read();
        // COMMENT(gwo): it is weired to use arrow types to identify the column belongs to label or scalar
        for column_id in projection.into_iter() {
            let field = columns
                .arrows
                .get(column_id)
                .ok_or(QueryError::NoSuchColumn { id: column_id })?;
            fields.push(field.clone());
            match field.data_type() {
                ArrowDataType::Utf8 => {
                    let label_column = columns.labels.get(field.name().as_str()).ok_or(
                        QueryError::NoSuchLabel {
                            name: field.name().to_string(),
                        },
                    )?;
                    arrow_arrays.push(Arc::new(ArrowStringArray::from(
                        ids.iter()
                            .cloned()
                            .map(|id| label_column.get(id))
                            .collect::<Vec<_>>(),
                    )));
                }
                ArrowDataType::List(_) => {
                    let scalar_column = columns.scalars.get(field.name().as_str()).ok_or(
                        QueryError::NoSuchScalar {
                            name: field.name().to_string(),
                        },
                    )?;
                    for series in ids.iter().cloned().map(|id| scalar_column.get(id).unwrap()) {
                        match series {
                            ScalarType::Int(series) => {
                                let mut builder = ListBuilder::new(Int64Builder::new(
                                    self.info.series_len as usize,
                                ));
                                for scalar in series.read().unwrap().iter() {
                                    builder.values().append_option(scalar).unwrap();
                                }
                                builder.append(true).unwrap();
                                arrow_arrays.push(Arc::new(builder.finish()));
                            }
                            ScalarType::Float(series) => {
                                let mut builder = ListBuilder::new(Float64Builder::new(
                                    self.info.series_len as usize,
                                ));
                                for scalar in series.read().unwrap().iter() {
                                    builder.values().append_option(scalar).unwrap();
                                }
                                builder.append(true).unwrap();
                                arrow_arrays.push(Arc::new(builder.finish()));
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        return RecordBatch::try_new(Arc::new(Schema::new(fields)), arrow_arrays)
            .map_err(|err| QueryError::DataFusionError { err });
    }

    fn on_read(&self) -> RwLockReadGuard<Columns> {
        return self.columns.read().unwrap();
    }

    fn on_write(&self) -> RwLockWriteGuard<Columns> {
        return self.columns.write().unwrap();
    }

    fn get_schema(&self) -> SchemaRef {
        let columns = self.on_read();
        return SchemaRef::from(Schema::new(columns.arrows.clone()));
    }

    fn datafusion_scan(
        &self,
        predicate: Expr,
        pre_filtered: Option<HashSet<usize>>,
    ) -> Result<HashSet<usize>, QueryError> {
        return if let Expr::BinaryExpr { left, op, right } = predicate {
            match op {
                ArrowOperator::And => {
                    let left = self.datafusion_scan(*left, None)?;
                    self.datafusion_scan(*right, Some(left))
                }
                ArrowOperator::Or => {
                    let left = self.datafusion_scan(*left, None)?;
                    let right = self.datafusion_scan(*right, None)?;
                    Ok(left.union(&right).cloned().collect())
                }
                ArrowOperator::Eq
                | ArrowOperator::NotEq
                | ArrowOperator::Like
                | ArrowOperator::NotLike => {
                    let columns = self.on_read();
                    if let box Expr::Column(column) = left {
                        let column_name = column.name.as_str();
                        if !columns.labels.contains_key(column_name) {
                            return Err(QueryError::NoSuchScalar { name: column.name });
                        }
                        match right {
                            box Expr::Literal(ScalarValue::Utf8(value)) => {
                                let filterer = match op {
                                    ArrowOperator::Eq => Matcher::LiteralEqual,
                                    ArrowOperator::NotEq => Matcher::LiteralNotEqual,
                                    ArrowOperator::Like => {
                                        |o: Option<&'a str>| Matcher::RegexMatch(o.unwrap())
                                    }
                                    ArrowOperator::NotLike => {
                                        |o: Option<&'a str>| Matcher::RegexNotMatch(o.unwrap())
                                    }
                                    _ => unreachable!(),
                                };
                                Ok(self.filter(
                                    Filter {
                                        name: column_name,
                                        matcher: filterer(value.as_deref()),
                                    },
                                    pre_filtered,
                                ))
                            }
                            _ => Err(QueryError::WrongFilterValue { expr: *right }),
                        }
                    } else {
                        Err(QueryError::WrongFiltered { expr: *left })
                    }
                }
                _ => Err(QueryError::WrongOperator { op }),
            }
        } else {
            Err(QueryError::WrongExpression { expr: predicate })
        };
    }
}

impl TableProvider for MutableChunk {
    fn as_any(&self) -> &dyn Any {
        return self;
    }

    fn schema(&self) -> SchemaRef {
        return self.get_schema();
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let columns = self.on_read();
        let column_ids = match projection {
            Some(ids) => ids.clone(),
            None => (0..columns.arrows.len()).collect(),
        };
        let predicate = combine_filters(filters);
        let record_ids = if let Some(predicate) = predicate {
            self.datafusion_scan(predicate, None)
                .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))?
        } else {
            (0..self.stat.record_num.load(Ordering::SeqCst)).collect()
        };
        let partition = self
            .to_arrow(column_ids, &record_ids)
            .map_err(|err| DataFusionError::Execution(format!("{:?}", err)))?;
        let schema = Arc::<datafusion::arrow::datatypes::Schema>::clone(&partition.schema());
        return Ok(Arc::new(MemoryExec::try_new(
            &[vec![partition]],
            schema,
            projection.clone(),
        )?));
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }

    fn has_exact_statistics(&self) -> bool {
        return true;
    }

    fn supports_filter_pushdown(&self, _: &Expr) -> DataFusionResult<TableProviderFilterPushDown> {
        return Ok(TableProviderFilterPushDown::Exact);
    }
}

#[derive(Debug)]
struct ChunkInfo {
    start_at: Instant,
    time_interval: Duration,
    series_len: u32,
}

impl ChunkInfo {
    fn new(start_at: Instant, time_interval: Duration, series_len: u32) -> Self {
        return Self {
            start_at,
            time_interval,
            series_len,
        };
    }
}

#[derive(Debug)]
struct ChunkStat {
    record_num: AtomicUsize,
}

impl ChunkStat {
    fn new() -> Self {
        return Self {
            record_num: AtomicUsize::new(0),
        };
    }

    fn add_record_num(&self, n: usize) -> usize {
        return self.record_num.fetch_add(n, Ordering::SeqCst);
    }

    fn get_record_num(&self) -> usize {
        return self.record_num.load(Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::{Filter, MutableChunk};
    use crate::storage::chunk::{Scalar, ScalarType};
    use crate::storage::query::Matcher;
    use datafusion::prelude::ExecutionContext;
    use hashbrown::HashMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn create_test_chunk(now: Instant) -> MutableChunk {
        let mut chunk = MutableChunk::new(now, Duration::SECOND, 128);
        chunk.set_schema(
            vec!["foo", "bar"],
            vec![ScalarType::Int("s1"), ScalarType::Float("s2")],
        );
        return chunk;
    }

    #[test]
    fn test_storage() {
        let now = Instant::now();
        let chunk = create_test_chunk(now);
        let labels = [("foo", "v1"), ("bar", "v2")]
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        chunk.lookup_or_insert(labels.clone()).insert(
            now + Duration::SECOND,
            [("s1", Scalar::Int(1))].iter().cloned().collect(),
        );
        assert_eq!(chunk.stat.record_num.load(Ordering::SeqCst), 1);
        let record = chunk.filter(
            Filter {
                name: "foo",
                matcher: Matcher::LiteralEqual(Some("v1")),
            },
            None,
        );
        assert_eq!(record.iter().cloned().next(), Some(0));
        let record = chunk.lookup_or_insert(labels.clone());
        assert_eq!(record.id, 0);
        let record =
            chunk.lookup_or_insert([("foo", "v1")].iter().cloned().collect::<HashMap<_, _>>());
        assert_eq!(record.id, 1);
        let record = chunk.filter(
            Filter {
                name: "foo",
                matcher: Matcher::LiteralEqual(Some("v1")),
            },
            None,
        );
        assert_eq!(record.len(), 2);
    }

    #[test]
    fn test_to_arrow() {
        let now = Instant::now();
        let chunk = create_test_chunk(now);
        println!("{:?}", chunk.get_schema());
    }

    #[tokio::test]
    async fn test_datafusion() {
        let now = Instant::now();
        let chunk = create_test_chunk(now);
        println!("{:?}", chunk.get_schema());
        let labels = [("foo", "v1"), ("bar", "v2")]
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        chunk.lookup_or_insert(labels.clone()).insert(
            now + Duration::SECOND,
            [("s1", Scalar::Int(1))].iter().cloned().collect(),
        );
        assert_eq!(chunk.stat.record_num.load(Ordering::SeqCst), 1);

        let mut ctx = ExecutionContext::new();
        ctx.register_table("data", Arc::new(chunk)).unwrap();
        let sql_results = ctx
            .sql("select * from data where foo = 'v1' and bar = 'v2'")
            .unwrap()
            .collect()
            .await
            .unwrap();
        println!("{:?}", sql_results);
    }
}
