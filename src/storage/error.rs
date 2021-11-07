use datafusion::arrow::error::ArrowError;
use datafusion::logical_plan::{Expr, Operator};
use snafu::Snafu;

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
    DataFusionError { err: ArrowError },
}
