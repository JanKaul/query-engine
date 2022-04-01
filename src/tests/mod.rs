use arrow2::{datatypes::Schema, io::parquet::read::schema};

use crate::{
    data_source::{DataSource, ParquetDataSource},
    dataframe::{DataFrame, DataFrameTrait},
    logical_plan::{format_logical_plan, Scan},
    prelude::*,
};

#[test]
fn parquet() {
    let df = DataFrame::parquet("src/tests/test.parquet")
        .filter(Box::new(col("id").eq(litString("Hugo"))));
    print!("{}", format_logical_plan(&df.logical_plan(), 0));
    assert_eq!("format_logical_plan(&df.logical_plan(), 0)", "");
}
