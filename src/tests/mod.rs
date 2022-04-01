use arrow2::{datatypes::Schema, io::parquet::read::schema};

use crate::{
    data_source::{DataSource, ParquetDataSource},
    logical_plan::{format_logical_plan, Scan},
};

#[test]
fn parquet() {
    let ds = ParquetDataSource::new("src/tests/test.parquet").unwrap();
    let scan = Scan::new("src/tests/test.parquet", ds, None);
    assert_eq!(format_logical_plan(&scan, 0), "");
}
