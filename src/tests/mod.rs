use arrow2::{array::PrimitiveArray, compute::arithmetics::ArrayAdd};

use crate::{
    dataframe::{DataFrame, DataFrameTrait},
    logical_plan::format_logical_plan,
    prelude::*,
};

#[test]
fn parquet() {
    let df = DataFrame::parquet("src/tests/test.parquet")
        .filter(Box::new(col("id").eq(lit_string("Hugo"))));
    assert_eq!(format_logical_plan(&df.logical_plan(), 0), "Selection: #id == 'Hugo',  \n \tScan: src/tests/test.parquet; projection=None \n");
}

#[test]
fn add() {
    let array1: PrimitiveArray<i32> = PrimitiveArray::from([Some(1), None, Some(3)]);
    let array2: PrimitiveArray<i32> = PrimitiveArray::from([None, Some(7), Some(11)]);
    let result = array1.add(&array2);
    assert_eq!(format!("{:?}",result), "Selection: #id == 'Hugo',  \n \tScan: src/tests/test.parquet; projection=None \n");
}
