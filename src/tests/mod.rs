use crate::{
    dataframe::{DataFrame, DataFrameTrait},
    logical_plan::format_logical_plan,
    prelude::*,
};

#[test]
fn parquet() {
    let df = DataFrame::parquet("src/tests/test.parquet")
        .filter(Box::new(col("id").eq(lit_string("Hugo"))));
    print!("{}", format_logical_plan(&df.logical_plan(), 0));
    assert_eq!("format_logical_plan(&df.logical_plan(), 0)", "");
}
