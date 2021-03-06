use crate::storage::error::{DBWriteError, QueryError};
use crate::storage::query::Matcher;
use crate::storage::util::dictionary::StringDictionary;
use async_recursion::async_recursion;
use datafusion::arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, ListBuilder, StringArray as ArrowStringArray,
};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema, SchemaRef,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::{Expr, Operator as ArrowOperator};
use datafusion::scalar::ScalarValue;
use hashbrown::HashMap;
use regex::Regex;
use roaring::RoaringBitmap;
use std::ops::{BitAnd, BitOr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime};
use tokio::join;

#[derive(Debug)]
pub struct Row<'a> {
    chunk: &'a mut MutableChunk,
    id: u32,
}

impl<'a> Row<'a> {
    fn new(chunk: &'a mut MutableChunk, id: u32) -> Self {
        return Self { id, chunk };
    }

    pub fn insert(&mut self, timestamp: SystemTime, scalars: &HashMap<String, Scalar>) {
        for (name, column) in self.chunk.columns.scalars.iter_mut() {
            let series = column.get_mut(self.id).unwrap();
            let scalar = scalars.get(name.as_ref()).cloned();
            let index = timestamp
                .duration_since(self.chunk.info.start_at)
                .unwrap()
                .div_duration_f64(self.chunk.info.time_interval) as u32;
            match series {
                ScalarType::Int(series) => series.insert(index, scalar.map(|s| s.into())),
                ScalarType::Float(series) => series.insert(index, scalar.map(|s| s.into())),
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
pub enum ScalarType<T, G> {
    Int(T),
    Float(G),
}

pub type Scalar = ScalarType<i64, f64>;

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

#[derive(Debug)]
struct ScalarColumn {
    series_len: u32,
    data: ScalarType<Vec<Series<i64>>, Vec<Series<f64>>>,
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
                column.resize_with(to as usize, || Series::new(self.series_len))
            }
            ScalarType::Float(column) => {
                column.resize_with(to as usize, || Series::new(self.series_len))
            }
        };
    }

    fn push_zero(&mut self) {
        match &mut self.data {
            ScalarType::Int(column) => column.push(Series::new(self.series_len)),
            ScalarType::Float(column) => column.push(Series::new(self.series_len)),
        };
    }

    fn get(&self, offset: u32) -> Option<ScalarType<&Series<i64>, &Series<f64>>> {
        return match &self.data {
            ScalarType::Int(data) => data.get(offset as usize).map(ScalarType::Int),
            ScalarType::Float(data) => data.get(offset as usize).map(ScalarType::Float),
        };
    }

    fn get_mut(&mut self, offset: u32) -> Option<ScalarType<&mut Series<i64>, &mut Series<f64>>> {
        return match &mut self.data {
            ScalarType::Int(data) => data.get_mut(offset as usize).map(ScalarType::Int),
            ScalarType::Float(data) => data.get_mut(offset as usize).map(ScalarType::Float),
        };
    }
}

#[derive(Debug, Default)]
struct LabelColumn {
    data: Vec<usize>,
    values: StringDictionary,
    index: HashMap<usize, RoaringBitmap>,
}

impl LabelColumn {
    fn new() -> Self {
        let mut hash_map = HashMap::new();
        hash_map.insert(0, RoaringBitmap::new());
        return Self {
            data: Vec::new(),
            values: StringDictionary::new(),
            index: hash_map,
        };
    }

    fn resize(&mut self, to: u32) {
        self.data.resize(to as usize, 0);
    }

    fn push(&mut self, s: &str) {
        let id = self.values.lookup_or_insert(s);
        self.data.push(id);
        let entry = self.index.entry(id);
        let bitmap = entry.or_insert_with(RoaringBitmap::new);
        bitmap.insert(self.data.len() as u32 - 1);
    }

    fn push_zero(&mut self) {
        self.data.push(0);
        self.index.entry(0).and_modify(|map| {
            map.insert(self.data.len() as u32 - 1);
        });
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
        pre_filtered: Option<&'a RoaringBitmap>,
    ) -> Option<RoaringBitmap> {
        let match_func: Box<dyn Fn(&(usize, usize)) -> bool> = match self.matcher {
            Matcher::LiteralEqual(v) => {
                let sid = match v {
                    Some(s) => column.values.lookup(s)?,
                    None => 0,
                };
                return Some(column.index.get(&sid).unwrap().clone());
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
        let filter_id: Box<dyn Iterator<Item = u32>> = match pre_filtered {
            Some(pre_filtered) => Box::new(pre_filtered.into_iter()),
            None => Box::new(0..column.data.len() as u32),
        };
        let iter = filter_id.map(|id| *column.data.get(id as usize).unwrap());
        return Some(
            iter.enumerate()
                .filter(match_func)
                .map(|(id, _)| id as u32)
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

    fn filter_label(&self, filter: Filter, pre_filtered: Option<RoaringBitmap>) -> RoaringBitmap {
        let records = self
            .labels
            .get(filter.name)
            .and_then(|column| filter.filter(column, pre_filtered.as_ref()))
            .map(|records| match pre_filtered {
                Some(pre_filtered) => pre_filtered.bitand(records),
                None => records,
            });
        return records.unwrap_or_default();
    }
}

#[derive(Debug)]
pub struct MutableChunk {
    pub info: ChunkInfo,
    stat: ChunkStat,

    columns: Columns,
}

impl<'a> MutableChunk {
    pub fn new(start_at: SystemTime, time_interval: Duration, series_len: u32) -> Self {
        return Self {
            info: ChunkInfo::new(start_at, time_interval, series_len),
            stat: ChunkStat::new(),
            columns: Columns::new(),
        };
    }

    pub fn set_schema(&mut self, labels: Vec<&str>, scalars: Vec<ScalarType<&str, &str>>) {
        for name in labels.into_iter() {
            self.columns
                .labels
                .insert(Arc::from(name), LabelColumn::new());
            self.columns
                .arrows
                .push(ArrowField::new(name, ArrowDataType::Utf8, true));
        }
        for scalar in scalars.into_iter() {
            let (name, scalar_type, arrow_type) = match scalar {
                ScalarType::Int(name) => (name, ScalarType::Int(()), ArrowDataType::Int64),
                ScalarType::Float(name) => (name, ScalarType::Float(()), ArrowDataType::Float64),
            };
            self.columns.scalars.insert(
                Arc::from(name),
                ScalarColumn::new(scalar_type, self.info.series_len),
            );
            self.columns.arrows.push(ArrowField::new(
                name,
                ArrowDataType::List(Box::new(ArrowField::new("item", arrow_type, true))),
                false,
            ));
        }
    }

    fn create_record(&'a mut self, labels: HashMap<String, String>) -> Row<'a> {
        for (name, column) in self.columns.labels.iter_mut() {
            let label_value = labels.get(name.as_ref());
            match label_value {
                Some(v) => column.push(v),
                None => column.push_zero(),
            }
        }

        for column in self.columns.scalars.values_mut() {
            column.push_zero();
        }

        return Row::new(self, self.stat.add_record_num(1));
    }

    fn filter(&self, filter: Filter, pre_filtered: Option<RoaringBitmap>) -> RoaringBitmap {
        return self.columns.filter_label(filter, pre_filtered);
    }

    pub fn test(&mut self) {}

    pub fn lookup_or_insert(&'a mut self, labels: HashMap<String, String>) -> Row<'a> {
        let mut pre_filtered = None;
        for column_name in self.columns.labels.keys() {
            pre_filtered = Some(self.columns.filter_label(
                Filter {
                    name: column_name,
                    matcher: Matcher::LiteralEqual(
                        labels.get(column_name.as_ref()).map(String::as_ref),
                    ),
                },
                pre_filtered,
            ));
        }
        return match pre_filtered {
            Some(set) => match set.iter().next() {
                Some(id) => Row::new(self, id),
                None => self.create_record(labels),
            },
            None => self.create_record(labels),
        };
    }

    fn to_arrow(
        &self,
        projection: Vec<usize>,
        ids: RoaringBitmap,
        start_at: SystemTime,
        end_at: SystemTime,
    ) -> Result<RecordBatch, QueryError> {
        let start_at = self.get_time_offset(start_at, 0) as usize;
        let end_at = self.get_time_offset(end_at, self.info.series_len) as usize;
        let mut fields = Vec::new();
        let mut arrow_arrays = Vec::<ArrayRef>::new();
        // COMMENT(gwo): it is weired to use arrow types to identify the column belongs to label or scalar
        for column_id in projection.into_iter() {
            let field = self
                .columns
                .arrows
                .get(column_id)
                .ok_or(QueryError::NoSuchColumn { id: column_id })?;
            fields.push(field.clone());
            match field.data_type() {
                ArrowDataType::Utf8 => {
                    let label_column = self.columns.labels.get(field.name().as_str()).ok_or(
                        QueryError::NoSuchLabel {
                            name: field.name().to_string(),
                        },
                    )?;
                    arrow_arrays.push(Arc::new(ArrowStringArray::from(
                        ids.iter()
                            .map(|id| label_column.get(id as usize))
                            .collect::<Vec<_>>(),
                    )));
                }
                ArrowDataType::List(_) => {
                    let scalar_column = self.columns.scalars.get(field.name().as_str()).ok_or(
                        QueryError::NoSuchScalar {
                            name: field.name().to_string(),
                        },
                    )?;
                    for series in ids.iter().map(|id| scalar_column.get(id).unwrap()) {
                        match series {
                            ScalarType::Int(series) => {
                                let mut builder = ListBuilder::new(Int64Builder::new(
                                    self.info.series_len as usize,
                                ));
                                for scalar in series.data[start_at..end_at].iter().cloned() {
                                    builder.values().append_option(scalar).unwrap();
                                }
                                builder.append(true).unwrap();
                                arrow_arrays.push(Arc::new(builder.finish()));
                            }
                            ScalarType::Float(series) => {
                                let mut builder = ListBuilder::new(Float64Builder::new(
                                    self.info.series_len as usize,
                                ));
                                for scalar in series.data[start_at..end_at].iter().cloned() {
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
            .map_err(|err| QueryError::ArrowError { err });
    }

    pub fn schema(&self) -> SchemaRef {
        return SchemaRef::from(Schema::new(self.columns.arrows.clone()));
    }

    #[async_recursion]
    async fn datafusion_scan(
        self: Arc<Self>,
        predicate: Box<Expr>,
        pre_filtered: Option<RoaringBitmap>,
    ) -> Result<RoaringBitmap, QueryError> {
        return if let box Expr::BinaryExpr { left, op, right } = predicate {
            match op {
                ArrowOperator::And => {
                    let left = Arc::clone(&self).datafusion_scan(left, None).await?;
                    Arc::clone(&self).datafusion_scan(right, Some(left)).await
                }
                ArrowOperator::Or => {
                    let (left, right) = join!(
                        tokio::spawn(Arc::clone(&self).datafusion_scan(left, None)),
                        tokio::spawn(Arc::clone(&self).datafusion_scan(right, None))
                    );
                    Ok(left.unwrap()?.bitor(&right.unwrap()?))
                }
                ArrowOperator::Eq
                | ArrowOperator::NotEq
                | ArrowOperator::Like
                | ArrowOperator::NotLike => {
                    if let box Expr::Column(column) = left {
                        let column_name = column.name.as_str();
                        if !self.columns.labels.contains_key(column_name) {
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
            Err(QueryError::WrongExpression { expr: *predicate })
        };
    }

    fn get_time_offset(&self, timestamp: SystemTime, default: u32) -> u32 {
        return timestamp
            .duration_since(self.info.start_at)
            .ok()
            .map(|duration| duration.div_duration_f64(self.info.time_interval) as u32)
            .unwrap_or(default);
    }

    pub async fn scan(
        self: Arc<Self>,
        projection: Arc<Vec<ArrowField>>,
        _batch_size: usize,
        filters: Arc<Option<Expr>>,
        _limit: Option<usize>,
        start_at: SystemTime,
        end_at: SystemTime,
    ) -> Result<RecordBatch, QueryError> {
        let column_ids = projection
            .iter()
            .filter_map(|field| {
                self.columns
                    .arrows
                    .iter()
                    .enumerate()
                    .find(|(_, f)| *f == field)
                    .map(|(id, _)| id)
            })
            .collect::<Vec<_>>();
        let record_ids = if let Some(filters) = filters.as_ref() {
            Arc::clone(&self)
                .datafusion_scan(Box::new(filters.clone()), None)
                .await?
        } else {
            (0..self.stat.record_num.load(Ordering::SeqCst)).collect()
        };
        return self.to_arrow(column_ids, record_ids, start_at, end_at);
    }
}

#[derive(Debug)]
pub struct ChunkInfo {
    pub start_at: SystemTime,
    time_interval: Duration,
    series_len: u32,
}

impl ChunkInfo {
    fn new(start_at: SystemTime, time_interval: Duration, series_len: u32) -> Self {
        return Self {
            start_at,
            time_interval,
            series_len,
        };
    }

    pub fn end_at(&self) -> SystemTime {
        return self.start_at + self.time_interval * (self.series_len - 1);
    }
}

#[derive(Debug)]
struct ChunkStat {
    record_num: AtomicU32,
}

impl ChunkStat {
    fn new() -> Self {
        return Self {
            record_num: AtomicU32::new(0),
        };
    }

    fn add_record_num(&self, n: u32) -> u32 {
        return self.record_num.fetch_add(n, Ordering::SeqCst);
    }

    fn get_record_num(&self) -> u32 {
        return self.record_num.load(Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::{Filter, MutableChunk};
    use crate::storage::chunk::{Scalar, ScalarType};
    use crate::storage::query::{make_range_udf, make_time_udf, Matcher};
    use hashbrown::HashMap;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, SystemTime};

    fn create_test_chunk(now: SystemTime) -> MutableChunk {
        let mut chunk = MutableChunk::new(now, Duration::SECOND, 128);
        chunk.set_schema(
            vec!["foo", "bar"],
            vec![ScalarType::Int("s1"), ScalarType::Float("s2")],
        );
        return chunk;
    }

    #[test]
    fn test_storage() {
        let now = SystemTime::now();
        let mut chunk = create_test_chunk(now);
        let labels = [
            (String::from("foo"), String::from("v1")),
            (String::from("bar"), String::from("v2")),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let scalars = [(String::from("s1"), Scalar::Int(1))]
            .iter()
            .cloned()
            .collect();
        chunk
            .lookup_or_insert(labels.clone())
            .insert(now + Duration::SECOND, &scalars);
        assert_eq!(chunk.stat.record_num.load(Ordering::SeqCst), 1);
        let record = chunk.filter(
            Filter {
                name: "foo",
                matcher: Matcher::LiteralEqual(Some("v1")),
            },
            None,
        );
        assert_eq!(record.iter().next(), Some(0));
        let record = chunk.lookup_or_insert(labels.clone());
        assert_eq!(record.id, 0);
        let record = chunk.lookup_or_insert(
            [(String::from("foo"), String::from("v1"))]
                .into_iter()
                .collect::<HashMap<_, _>>(),
        );
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
        let now = SystemTime::now();
        let chunk = create_test_chunk(now);
        println!("{:?}", chunk.schema());
    }
}
