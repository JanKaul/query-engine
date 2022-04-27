
use crate::{
    dataframe::{DataFrame, DataFrameTrait},
    logical_plan::format_logical_plan,
    prelude::*,
};

#[test]
fn parquet() {
    let df = DataFrame::parquet("src/tests/test.parquet")
        .filter(col("id").eq(lit_string("Hugo")));
    assert_eq!(format_logical_plan(&df.logical_plan(), 0), "Selection: #id == 'Hugo',  \n \tScan: src/tests/test.parquet; projection=None \n");
}
