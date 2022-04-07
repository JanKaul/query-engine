use arrow2::{
    array::{PrimitiveArray, Utf8Array},
    datatypes::PhysicalType::{self},
    scalar::{PrimitiveScalar, Utf8Scalar},
};
use std::sync::Arc;

use arrow2::{array::Array, datatypes::PrimitiveType, scalar::Scalar};

use crate::error::Error;

pub enum ColumnarValue {
    Array(Arc<dyn Array>),
    Scalar(Box<dyn Scalar>),
}

pub fn scalar_to_array(scalar: Box<dyn Scalar>, len: usize) -> Result<Arc<dyn Array>, Error> {
    match scalar.data_type().to_physical_type() {
        PhysicalType::Primitive(PrimitiveType::Int32) => scalar
            .as_any()
            .downcast_ref::<PrimitiveScalar<i32>>()
            .and_then(|x| x.value())
            .map(|val| Arc::new(PrimitiveArray::from_vec(vec![val; len])) as Arc<dyn Array>)
            .ok_or(Error::ScalarToArrayError(format!("{:?}", scalar))),
        PhysicalType::Primitive(PrimitiveType::Float64) => scalar
            .as_any()
            .downcast_ref::<PrimitiveScalar<f64>>()
            .and_then(|x| x.value())
            .map(|val| Arc::new(PrimitiveArray::from_vec(vec![val; len])) as Arc<dyn Array>)
            .ok_or(Error::ScalarToArrayError(format!("{:?}", scalar))),
        PhysicalType::Utf8 => scalar
            .as_any()
            .downcast_ref::<Utf8Scalar<i32>>()
            .and_then(|x| x.value())
            .map(|val| {
                Arc::new(Utf8Array::<i32>::from::<&str, &Vec<Option<&str>>>(&vec![
                    Some(val);
                    len
                ])) as Arc<dyn Array>
            })
            .ok_or(Error::ScalarToArrayError(format!("{:?}", scalar))),
        _ => Err(Error::ScalarToArrayError(format!("{:?}", scalar))),
    }
}
