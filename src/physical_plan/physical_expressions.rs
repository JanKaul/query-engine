use std::borrow::Borrow;
use std::fmt::{self, Display};
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;

use arrow2::array::{BooleanArray, Utf8Array};
use arrow2::bitmap::Bitmap;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, PhysicalType};
use arrow2::scalar::{BooleanScalar, NullScalar, PrimitiveScalar, Scalar};
use arrow2::{
    array::{Array, PrimitiveArray},
    compute,
    datatypes::PrimitiveType,
    scalar::Utf8Scalar,
};
use std::ops::BitAnd;

use crate::columnar_value::ColumnarValue;
use crate::error::Error;

pub trait PhysicalExpression: Display {
    fn evaluate(&self, input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error>;
}

#[derive(Clone)]
pub struct ColumnExpression {
    pub index: usize,
}

impl PhysicalExpression for ColumnExpression {
    fn evaluate(&self, input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
        input
            .get(self.index)
            .and_then(|x| {
                let x: &dyn Array = x.borrow();
                match x.data_type().to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::Int32) => x
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .map(|y| ColumnarValue::Array(Arc::new(y.clone()) as Arc<dyn Array>)),
                    PhysicalType::Primitive(PrimitiveType::Float64) => x
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .map(|y| ColumnarValue::Array(Arc::new(y.clone()) as Arc<dyn Array>)),
                    PhysicalType::Utf8 => x
                        .as_any()
                        .downcast_ref::<Utf8Array<i32>>()
                        .map(|y| ColumnarValue::Array(Arc::new(y.clone()) as Arc<dyn Array>)),
                    PhysicalType::Boolean => x
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .map(|y| ColumnarValue::Array(Arc::new(y.clone()) as Arc<dyn Array>)),
                    _ => None,
                }
            })
            .ok_or(Error::PrimitiveTypeNotSuported(format!(
                "{:?}",
                PrimitiveType::Int32
            )))
    }
}

impl fmt::Display for ColumnExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.index)
    }
}

#[derive(Clone)]
pub struct LiteralBoolExpression {
    pub value: BooleanScalar,
}

impl LiteralBoolExpression {
    pub fn new(value: bool) -> Self {
        LiteralBoolExpression {
            value: BooleanScalar::new(Some(value)),
        }
    }
}

impl PhysicalExpression for LiteralBoolExpression {
    fn evaluate(&self, _input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value.clone())))
    }
}

impl fmt::Display for LiteralBoolExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

#[derive(Clone)]
pub struct LiteralStringExpression {
    pub value: Utf8Scalar<i32>,
}

impl LiteralStringExpression {
    pub fn new(value: String) -> Self {
        LiteralStringExpression {
            value: Utf8Scalar::new(Some(value)),
        }
    }
}

impl PhysicalExpression for LiteralStringExpression {
    fn evaluate(&self, _input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value.clone())))
    }
}

impl fmt::Display for LiteralStringExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

#[derive(Clone)]
pub struct LiteralIntegerExpression {
    pub(crate) value: PrimitiveScalar<i32>,
}

impl LiteralIntegerExpression {
    pub fn new(value: i32) -> Self {
        LiteralIntegerExpression {
            value: PrimitiveScalar::new(DataType::Int32, Some(value)),
        }
    }
}

impl PhysicalExpression for LiteralIntegerExpression {
    fn evaluate(&self, _input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value.clone())))
    }
}

impl fmt::Display for LiteralIntegerExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

#[derive(Clone)]
pub struct LiteralFloatExpression {
    pub(crate) value: PrimitiveScalar<f64>,
}

impl LiteralFloatExpression {
    pub fn new(value: f64) -> Self {
        LiteralFloatExpression {
            value: PrimitiveScalar::new(DataType::Float64, Some(value)),
        }
    }
}

impl PhysicalExpression for LiteralFloatExpression {
    fn evaluate(&self, _input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
        Ok(ColumnarValue::Scalar(Box::new(self.value.clone())))
    }
}

impl fmt::Display for LiteralFloatExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{:?}", self.value)
    }
}

macro_rules! comparisonExpression {
    ($i: ident, $name1: ident, $name2: ident, $op: ident, $op_name: expr) => {
        pub struct $i {
            left: Box<dyn PhysicalExpression>,
            right: Box<dyn PhysicalExpression>,
        }

        impl PhysicalExpression for $i {
            fn evaluate(&self, input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
                let l = self.left.evaluate(input)?;
                let r = self.right.evaluate(input)?;
                match (l, r) {
                    (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
                        if left.len() == right.len() {
                            Ok(ColumnarValue::Array(Arc::new(compute::comparison::$name1(
                                &*left, &*right,
                            ))))
                        } else {
                            Err(Error::DifferentSizes(
                                format!("{:?}", left),
                                format!("{:?}", right),
                            ))
                        }
                    }
                    (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => {
                        Ok(ColumnarValue::Array(Arc::new(compute::comparison::$name2(
                            &*left, &*right,
                        ))))
                    }
                    (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => {
                        Ok(ColumnarValue::Array(Arc::new(compute::comparison::$name2(
                            &*right, &*left,
                        ))))
                    }
                    (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
                        Ok(ColumnarValue::Scalar(Box::new(BooleanScalar::new(Some(
                            left.$op(&*right),
                        )))))
                    }
                }
            }
        }

        impl $i {
            pub fn new(
                left: Box<dyn PhysicalExpression>,
                right: Box<dyn PhysicalExpression>,
            ) -> Self {
                $i {
                    left: left,
                    right: right,
                }
            }
        }

        impl fmt::Display for $i {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {} {}", self.left, $op_name, self.right)
            }
        }
    };
}
comparisonExpression!(EqExpression, eq, eq_scalar, eq, "==".to_string());
comparisonExpression!(NeqExpression, neq, neq_scalar, ne, "!=".to_string());

macro_rules! mathExpression {
    ($i: ident, $name1: ident, $name2: ident, $op: ident, $op_name: expr) => {
        pub struct $i {
            left: Box<dyn PhysicalExpression>,
            right: Box<dyn PhysicalExpression>,
        }

        impl PhysicalExpression for $i {
            fn evaluate(&self, input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
                let left = self.left.evaluate(input)?;
                let right = self.right.evaluate(input)?;
                match (left, right) {
                    (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
                        if left.len() == right.len() {
                            Ok(ColumnarValue::Array(Arc::from(
                                compute::arithmetics::$name1(&*left, &*right),
                            )))
                        } else {
                            Err(Error::DifferentSizes(
                                format!("{:?}", left),
                                format!("{:?}", right),
                            ))
                        }
                    }
                    (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => {
                        Ok(ColumnarValue::Array(Arc::from(
                            compute::arithmetics::$name2(&*left, &*right),
                        )))
                    }
                    (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => {
                        Ok(ColumnarValue::Array(Arc::from(
                            compute::arithmetics::$name2(&*right, &*left),
                        )))
                    }
                    (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
                        match (
                            left.data_type().to_physical_type(),
                            right.data_type().to_physical_type(),
                        ) {
                            (
                                PhysicalType::Primitive(PrimitiveType::Float64),
                                PhysicalType::Primitive(PrimitiveType::Float64),
                            ) => {
                                let (left, right) = (
                                    left.as_any()
                                        .downcast_ref::<PrimitiveScalar<f64>>()
                                        .ok_or(Error::DowncastError)?,
                                    right
                                        .as_any()
                                        .downcast_ref::<PrimitiveScalar<f64>>()
                                        .ok_or(Error::DowncastError)?,
                                );
                                Ok(ColumnarValue::Scalar(Box::new(PrimitiveScalar::new(
                                    DataType::Float64,
                                    match (left.value(), right.value()) {
                                        (Some(left), Some(right)) => Some(left.$op(right)),
                                        _ => None,
                                    },
                                ))))
                            }
                            (
                                PhysicalType::Primitive(PrimitiveType::Int32),
                                PhysicalType::Primitive(PrimitiveType::Int32),
                            ) => {
                                let (left, right) = (
                                    left.as_any()
                                        .downcast_ref::<PrimitiveScalar<i32>>()
                                        .ok_or(Error::DowncastError)?,
                                    right
                                        .as_any()
                                        .downcast_ref::<PrimitiveScalar<i32>>()
                                        .ok_or(Error::DowncastError)?,
                                );
                                Ok(ColumnarValue::Scalar(Box::new(PrimitiveScalar::new(
                                    DataType::Int32,
                                    match (left.value(), right.value()) {
                                        (Some(left), Some(right)) => Some(left.$op(right)),
                                        _ => None,
                                    },
                                ))))
                            }
                            _ => Err(Error::PrimitiveTypeNotSuported(format!(
                                "{:?}",
                                left.data_type()
                            ))),
                        }
                    }
                }
            }
        }

        impl $i {
            pub fn new(
                left: Box<dyn PhysicalExpression>,
                right: Box<dyn PhysicalExpression>,
            ) -> Self {
                $i {
                    left: left,
                    right: right,
                }
            }
        }

        impl fmt::Display for $i {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {} {}", self.left, $op_name, self.right)
            }
        }
    };
}

mathExpression!(AddExpression, add, add_scalar, add, "+".to_string());
mathExpression!(SubExpression, sub, sub_scalar, sub, "-".to_string());
mathExpression!(MulExpression, mul, mul_scalar, mul, "*".to_string());
mathExpression!(DivExpression, div, div_scalar, div, "/".to_string());

pub trait Accumulator {
    fn accumulate(
        &mut self,
        input: &Vec<ColumnarValue>,
        validity: Option<&Bitmap>,
    ) -> Result<(), Error>;
    fn final_value(self: Box<Self>) -> Result<ColumnarValue, Error>;
}

pub trait PhysicalAggregateExpression: PhysicalExpression {
    fn create_accumulator(&self, index: usize) -> Box<dyn Accumulator>;
}

macro_rules! aggregateExpression {
    ($acc: ident,$expr: ident, $name1: ident, $name2: ident, $op_name: expr) => {
        pub struct $acc {
            value: Box<dyn Scalar>,
            index: usize,
        }

        impl Accumulator for $acc {
            fn accumulate(
                &mut self,
                input: &Vec<ColumnarValue>,
                validity: Option<&Bitmap>,
            ) -> Result<(), Error> {
                let expr = &input[self.index];
                let new = match expr {
                    ColumnarValue::Array(expr) => {
                        let val = match (expr.validity(), validity) {
                            (Some(val1), Some(val2)) => Some(val1.bitand(val2)),
                            (Some(val), None) => Some(val.clone()),
                            (None, Some(val)) => Some(val.clone()),
                            (None, None) => None,
                        };
                        compute::aggregate::$name1(
                            &*(expr.borrow() as &dyn Array).with_validity(val),
                        )
                        .map_err(|err| Error::ArrowError(err))
                    }
                    ColumnarValue::Scalar(scalar) => match scalar.data_type().to_physical_type() {
                        PhysicalType::Primitive(PrimitiveType::Float64) => Ok(Box::new(
                            scalar
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<f64>>()
                                .ok_or(Error::DowncastError)?
                                .clone(),
                        )
                            as Box<dyn Scalar>),
                        PhysicalType::Primitive(PrimitiveType::Int32) => Ok(Box::new(
                            scalar
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<i32>>()
                                .ok_or(Error::DowncastError)?
                                .clone(),
                        )
                            as Box<dyn Scalar>),
                        x => Err(Error::PhysicalTypeNotSuported(format!("{:?}", x))),
                    },
                }?;
                let bool = match (
                    new.data_type().to_physical_type(),
                    self.value.data_type().to_physical_type(),
                ) {
                    (
                        PhysicalType::Primitive(PrimitiveType::Float64),
                        PhysicalType::Primitive(PrimitiveType::Float64),
                    ) => {
                        let (left, right) = (
                            new.as_any()
                                .downcast_ref::<PrimitiveScalar<f64>>()
                                .ok_or(Error::DowncastError)?,
                            self.value
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<f64>>()
                                .ok_or(Error::DowncastError)?,
                        );
                        match (left.value(), right.value()) {
                            (Some(left), Some(right)) => Ok(left.$name2(&right)),
                            _ => Err(Error::DowncastError),
                        }
                    }
                    (
                        PhysicalType::Primitive(PrimitiveType::Int32),
                        PhysicalType::Primitive(PrimitiveType::Int32),
                    ) => {
                        let (left, right) = (
                            new.as_any()
                                .downcast_ref::<PrimitiveScalar<i32>>()
                                .ok_or(Error::DowncastError)?,
                            self.value
                                .as_any()
                                .downcast_ref::<PrimitiveScalar<i32>>()
                                .ok_or(Error::DowncastError)?,
                        );
                        match (left.value(), right.value()) {
                            (Some(left), Some(right)) => Ok(left.$name2(&right)),
                            _ => Err(Error::DowncastError),
                        }
                    }
                    _ => Err(Error::PrimitiveTypeNotSuported(format!(
                        "{:?}",
                        new.data_type()
                    ))),
                }?;
                if bool {
                    self.value = new;
                };
                Ok(())
            }
            fn final_value(self: Box<Self>) -> Result<ColumnarValue, Error> {
                Ok(ColumnarValue::Scalar(self.value))
            }
        }
        pub struct $expr {
            expr: Box<dyn PhysicalExpression>,
        }

        impl PhysicalExpression for $expr {
            fn evaluate(&self, input: &Chunk<Arc<dyn Array>>) -> Result<ColumnarValue, Error> {
                self.expr.evaluate(input)
            }
        }

        impl PhysicalAggregateExpression for $expr {
            fn create_accumulator(&self, index: usize) -> Box<dyn Accumulator> {
                Box::new($acc {
                    value: Box::new(NullScalar::new()),
                    index: index,
                })
            }
        }

        impl $expr {
            pub fn new(expr: Box<dyn PhysicalExpression>) -> Self {
                $expr { expr: expr }
            }
        }

        impl fmt::Display for $expr {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {}", $op_name, self.expr)
            }
        }
    };
}

aggregateExpression!(MaxAccumulator, MaxExpression, max, gt, "max".to_string());
aggregateExpression!(MinAccumulator, MinExpression, min, lt, "min".to_string());
// aggregateExpression!(SumAccumulator, SumExpression, sum, add, "sum".to_string());
