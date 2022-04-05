use arrow2::{array::Array, scalar::Scalar};

pub enum ColumnarValue {
    Array(Box<dyn Array>),
    Scalar(Box<dyn Scalar>),
}
