use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::functions::{make_scalar_function, Volatility};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::prelude::create_udf;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum Matcher<'a> {
    LiteralEqual(Option<&'a str>),
    LiteralNotEqual(Option<&'a str>),
    RegexMatch(&'a str),
    RegexNotMatch(&'a str),
}

pub fn make_range_udf() -> ScalarUDF {
    return create_udf(
        "range",
        vec![DataType::Null, DataType::Null, DataType::Null],
        Arc::new(DataType::Null),
        Volatility::Volatile,
        make_scalar_function(|_: &[ArrayRef]| unreachable!()),
    );
}

pub fn make_time_udf() -> ScalarUDF {
    return create_udf(
        "datetime",
        vec![DataType::Utf8],
        Arc::new(DataType::Date64),
        Volatility::Immutable,
        make_scalar_function(|args: &[ArrayRef]| Ok(Arc::clone(&args[0]))),
    );
}
