use std::fmt::{self, Display};

use arrow2::datatypes::DataType;

use crate::{
    column_vector::{ColumnVector, LiteralValueVector},
    error::Error,
    record_batch::RecordBatch,
};

trait Expression<T, V: ColumnVector<DataType = T>>: Display {
    fn evaluate<'a>(&'a self, input: &'a RecordBatch<T, V>) -> Result<&'a V, Error>;
}

pub struct ColumnExpression {
    index: usize,
}

impl<T, V: ColumnVector<DataType = T>> Expression<T, V> for ColumnExpression {
    fn evaluate<'a>(&'a self, input: &'a RecordBatch<T, V>) -> Result<&'a V, Error> {
        input.field(self.index)
    }
}

impl fmt::Display for ColumnExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.index)
    }
}

pub struct LiteralStringExpression {
    value: LiteralValueVector<String>,
}

impl LiteralStringExpression {
    pub fn new(value: String) -> Self {
        LiteralStringExpression {
            value: LiteralValueVector::new(DataType::Utf8, value),
        }
    }
}

impl Expression<String, LiteralValueVector<String>> for LiteralStringExpression {
    fn evaluate<'a>(
        &'a self,
        _input: &'a RecordBatch<String, LiteralValueVector<String>>,
    ) -> Result<&'a LiteralValueVector<String>, Error> {
        Ok(&self.value)
    }
}

impl fmt::Display for LiteralStringExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.value.get_value(0).unwrap())
    }
}

pub struct LiteralIntegerExpression {
    value: LiteralValueVector<i32>,
}

impl LiteralIntegerExpression {
    pub fn new(value: i32) -> Self {
        LiteralIntegerExpression {
            value: LiteralValueVector::new(DataType::Utf8, value),
        }
    }
}

impl Expression<i32, LiteralValueVector<i32>> for LiteralIntegerExpression {
    fn evaluate<'a>(
        &'a self,
        _input: &'a RecordBatch<i32, LiteralValueVector<i32>>,
    ) -> Result<&'a LiteralValueVector<i32>, Error> {
        Ok(&self.value)
    }
}

impl fmt::Display for LiteralIntegerExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.value.get_value(0).unwrap())
    }
}

pub struct LiteralFloatExpression {
    value: LiteralValueVector<f64>,
}

impl LiteralFloatExpression {
    pub fn new(value: f64) -> Self {
        LiteralFloatExpression {
            value: LiteralValueVector::new(DataType::Utf8, value),
        }
    }
}

impl Expression<f64, LiteralValueVector<f64>> for LiteralFloatExpression {
    fn evaluate<'a>(
        &'a self,
        _input: &'a RecordBatch<f64, LiteralValueVector<f64>>,
    ) -> Result<&'a LiteralValueVector<f64>, Error> {
        Ok(&self.value)
    }
}

impl fmt::Display for LiteralFloatExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.value.get_value(0).unwrap())
    }
}
