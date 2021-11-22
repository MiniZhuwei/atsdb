use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, LogicalPlan, Operator};
use snafu::Snafu;
use std::time::SystemTime;

#[derive(Snafu, Debug)]
pub enum QueryError {
    #[snafu(display("{:?} is not a valid filter expression", expr))]
    WrongExpression { expr: Expr },
    #[snafu(display("{} is not a valid filter operator", op))]
    WrongOperator { op: Operator },
    #[snafu(display("filtered expression should be a column, rather than: {:?}", expr))]
    WrongFiltered { expr: Expr },
    #[snafu(display("filter value expression should be a string, rather than: {:?}", expr))]
    WrongFilterValue { expr: Expr },
    #[snafu(display("table does not have such label column: {}", name))]
    NoSuchLabel { name: String },
    #[snafu(display("table does not have such column: {}", id))]
    NoSuchColumn { id: usize },
    #[snafu(display("table does not have such scalar column: {}", name))]
    NoSuchScalar { name: String },
    #[snafu(display("datafusion error: {}", err))]
    ArrowError { err: ArrowError },
    #[snafu(display("query must specify time ranging"))]
    NoTimeRange,
    #[snafu(display("wrong time range expression: {}", desc))]
    WrongTimeRange { desc: String },
}

#[derive(Snafu, Debug)]
pub enum DBError {
    #[snafu(display("datafusion error: {}", err))]
    InternalError { err: DataFusionError },
    #[snafu(display("only support filter sql, not: {:?}", plan))]
    NoSupportLogicalPlan { plan: LogicalPlan },
    #[snafu(display("only support filter sql, not: {:?}", err))]
    OtherError { err: String },
}

#[derive(Snafu, Debug)]
pub enum DBWriteError {
    #[snafu(display(
        "the timestamp: {:?} can not be insert, because the chunk is archived",
        timestamp
    ))]
    ChunkArchived { timestamp: SystemTime },

    #[snafu(display("update schema when error: {:?}", err))]
    UpdateSchemaError { err: ArrowError },

    #[snafu(display("db write internal error: {:?}", desc))]
    DBInternalError { desc: String },
}
