use crate::{
    dataframe::{DataFrame, DataFrameTrait},
    logical_plan::format_logical_plan,
    prelude::*,
};

#[test]
fn test_parquet() {
    let df = DataFrame::parquet("src/tests/test.parquet").filter(col("id").eq(lit_int(4)));
    assert_eq!(
        format_logical_plan(&df.logical_plan(), 0),
        "Selection: #id == '4',  \n \tScan: src/tests/test.parquet; projection=None \n"
    );
}

#[test]
fn test_schema() {
    let df = DataFrame::parquet("src/tests/test.parquet");
    assert_eq!(format!("{}", df.schema().fields[1].name), "bool_col");
}

#[test]
fn test_filter() {
    let result = DataFrame::parquet("src/tests/test.parquet")
        .filter(col("id").eq(lit_int(3)))
        .execute()
        .unwrap();
    assert_eq!(format!("{:?}", result[0][4]), "Int32[1]");
}

#[test]
fn test_max() {
    let result = DataFrame::parquet("src/tests/test.parquet")
        .aggregate(vec![col("bool_col")], vec![max(col("id"))])
        .execute()
        .unwrap();
    assert_eq!(format!("{:?}", result[0][1]), "Int32[7, 6]");
}

#[test]
fn test_projection_push_down() {
    let df = DataFrame::parquet("src/tests/test.parquet")
        .project(vec![col("id")])
        .filter(col("id").eq(lit_int(4)));
    assert_eq!(
        format_logical_plan(&df.logical_plan().optimize(), 0),
        "Selection: #id == '4',  \n \tProjection: #id,  \n \t \tScan: src/tests/test.parquet; projection=id \n"
    );
}
