use arrow2::array::{PrimitiveArray, Utf8Array};

use crate::{
    dataframe::{DataFrame, DataFrameTrait},
    prelude::*,
};

// userdata1.parquet: These are sample files containing data in PARQUET format.

// -> Number of rows in each file: 1000
// -> Column details:
// column#		column_name		hive_datatype
// =====================================================
// 1		registration_dttm 	timestamp
// 2		id 			int
// 3		first_name 		string
// 4		last_name 		string
// 5		email 			string
// 6		gender 			string
// 7		ip_address 		string
// 8		cc 			string
// 9		country 		string
// 10		birthdate 		string
// 11		salary 			double
// 12		title 			string
// 13		comments 		string

#[test]
fn test_schema() {
    let df = DataFrame::parquet("src/tests/userdata.parquet");
    assert_eq!(format!("{:?}", df.schema().fields), "[Field { name: \"registration_dttm\", data_type: Timestamp(Nanosecond, None), is_nullable: true, metadata: {} }, Field { name: \"id\", data_type: Int32, is_nullable: true, metadata: {} }, Field { name: \"first_name\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"last_name\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"email\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"gender\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"ip_address\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"cc\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"country\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"birthdate\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"salary\", data_type: Float64, is_nullable: true, metadata: {} }, Field { name: \"title\", data_type: Utf8, is_nullable: true, metadata: {} }, Field { name: \"comments\", data_type: Utf8, is_nullable: true, metadata: {} }]");
}

#[test]
fn test_scan() {
    let result = DataFrame::parquet("src/tests/userdata.parquet")
        .execute()
        .expect("Failed to execute parquet scan.");

    assert_eq!(
        format!(
            "{:?}",
            result[0][2]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"Amanda\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][3]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"Jordan\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][2]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"Albert\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][3]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"Freeman\""
    );
}

#[test]
fn test_projection() {
    let result = DataFrame::parquet("src/tests/userdata.parquet")
        .project(vec![col("email"), col("country")])
        .execute()
        .unwrap();
    assert_eq!(
        format!(
            "{:?}",
            result[0][0]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"ajordan0@com.com\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][1]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"Indonesia\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][0]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"afreeman1@is.gd\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][1]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"Canada\""
    );
}

#[test]
fn test_filter() {
    let result = DataFrame::parquet("src/tests/userdata.parquet")
        .filter(col("gender").eq(lit_string("Female")))
        .execute()
        .unwrap();
    assert_eq!(
        format!(
            "{:?}",
            result[0][2]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"Amanda\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][3]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(0)
        ),
        "\"Jordan\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][2]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"Evelyn\""
    );
    assert_eq!(
        format!(
            "{:?}",
            result[0][3]
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .expect("Failed to downcast array to utf8 array.")
                .value(1)
        ),
        "\"Morgan\""
    );
}

#[test]
fn test_max() {
    let result = DataFrame::parquet("src/tests/userdata.parquet")
        .aggregate(vec![col("country")], vec![max(col("salary"))])
        .execute()
        .unwrap();
    assert_eq!(
        format!(
            "{:?}",
            result[0][0]
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap()
                .value(0)
        ),
        "Int32[6, 7]"
    );
}
