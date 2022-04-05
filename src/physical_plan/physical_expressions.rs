use std::fmt::{self, Display};

use arrow2::datatypes::DataType;
use arrow2::datatypes::PhysicalType::Primitive;
use arrow2::scalar::{BooleanScalar, PrimitiveScalar};
use arrow2::{
    array::{Array, PrimitiveArray},
    compute,
    datatypes::PrimitiveType,
    scalar::Utf8Scalar,
};

use crate::columnar_value::ColumnarValue;
use crate::{error::Error, record_batch::RecordBatch};

pub trait Expression: Display {
    fn evaluate(self, input: &RecordBatch) -> Result<ColumnarValue, Error>;
}

pub struct ColumnExpression {
    index: usize,
}

impl Expression for ColumnExpression {
    fn evaluate(self, input: &RecordBatch) -> Result<ColumnarValue, Error> {
        input
            .field(self.index)
            .and_then(|x| match x.data_type().to_physical_type() {
                Primitive(PrimitiveType::Int32) => x
                    .as_any()
                    .downcast_ref::<PrimitiveArray<i32>>()
                    .ok_or(Error::PrimitiveTypeNotSuported(format!(
                        "{:?}",
                        PrimitiveType::Int32
                    )))
                    .map(|y| ColumnarValue::Array(Box::new(y.clone()) as Box<dyn Array>)),
                t => Err(Error::PhysicalTypeNotSuported(format!("{:?}", t))),
            })
    }
}

impl fmt::Display for ColumnExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.index)
    }
}

pub struct LiteralStringExpression {
    value: Utf8Scalar<i32>,
}

impl LiteralStringExpression {
    pub fn new(value: String) -> Self {
        LiteralStringExpression {
            value: Utf8Scalar::new(Some(value)),
        }
    }
}

impl Expression for LiteralStringExpression {
    fn evaluate(self, _input: &RecordBatch) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value)))
    }
}

impl fmt::Display for LiteralStringExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

pub struct LiteralIntegerExpression {
    value: PrimitiveScalar<i32>,
}

impl LiteralIntegerExpression {
    pub fn new(value: i32) -> Self {
        LiteralIntegerExpression {
            value: PrimitiveScalar::new(DataType::Int32, Some(value)),
        }
    }
}

impl Expression for LiteralIntegerExpression {
    fn evaluate(self, _input: &RecordBatch) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value)))
    }
}

impl fmt::Display for LiteralIntegerExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

pub struct LiteralFloatExpression {
    value: PrimitiveScalar<f64>,
}

impl LiteralFloatExpression {
    pub fn new(value: f64) -> Self {
        LiteralFloatExpression {
            value: PrimitiveScalar::new(DataType::Float64, Some(value)),
        }
    }
}

impl Expression for LiteralFloatExpression {
    fn evaluate(self, _input: &RecordBatch) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value)))
    }
}

impl fmt::Display for LiteralFloatExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

pub struct EqExpression<E: Expression> {
    left: E,
    right: E,
}

impl<E: Expression> Expression for EqExpression<E> {
    fn evaluate(self, input: &RecordBatch) -> Result<ColumnarValue, Error> {
        let l = self.left.evaluate(input)?;
        let r = self.right.evaluate(input)?;
        match (l, r) {
            (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
                if left.len() == right.len() {
                    Ok(ColumnarValue::Array(Box::new(compute::comparison::eq(
                        &*left, &*right,
                    ))))
                } else {
                    Err(Error::DifferentSizes(
                        format!("{:?}", left),
                        format!("{:?}", right),
                    ))
                }
            }
            (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => Ok(ColumnarValue::Array(
                Box::new(compute::comparison::eq_scalar(&*left, &*right)),
            )),
            (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => Ok(ColumnarValue::Array(
                Box::new(compute::comparison::eq_scalar(&*right, &*left)),
            )),
            (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => Ok(
                ColumnarValue::Scalar(Box::new(BooleanScalar::new(Some(left == right)))),
            ),
        }
    }
}

impl<E: Expression> EqExpression<E> {
    pub fn new(left: E, right: E) -> Self {
        EqExpression {
            left: left,
            right: right,
        }
    }
}

impl<E: Expression> fmt::Display for EqExpression<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} == {}", self.left, self.right)
    }
}
