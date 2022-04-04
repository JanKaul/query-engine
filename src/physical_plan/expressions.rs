use std::{
    fmt::{self, Display},
    marker::PhantomData,
};

use arrow2::datatypes::DataType;

use crate::{
    column_vector::{ColumnVector, LiteralValueVector},
    error::Error,
    record_batch::RecordBatch,
};

trait Expression: Display {
    type InputType;
    type InputVector: ColumnVector<DataType = Self::InputType>;
    type OutputType;
    type OutputVector: ColumnVector<DataType = Self::InputType>;
    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error>;
}

pub struct ColumnExpression<
    TI,
    VI: ColumnVector<DataType = TI>,
    TO,
    VO: ColumnVector<DataType = TO>,
> {
    index: usize,
    input: PhantomData<VI>,
    output: PhantomData<VO>,
}

impl<TI, VI: ColumnVector<DataType = TI>, TO, VO: ColumnVector<DataType = TO>> Expression
    for ColumnExpression<TI, VI, TO, VO>
{
    type InputType = TI;
    type InputVector = VI;
    type OutputType = TO;
    type OutputVector = VO;

    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error> {
        input.field(self.index)
    }
}

impl<T, V: ColumnVector<DataType = T>> fmt::Display for ColumnExpression<T, V> {
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

impl Expression for LiteralStringExpression {
    type InputType = String;
    type InputVector = LiteralValueVector<String>;
    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error> {
        Ok(self.value)
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

impl Expression for LiteralIntegerExpression {
    type InputType = i32;
    type InputVector = LiteralValueVector<i32>;
    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error> {
        Ok(self.value)
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

impl Expression for LiteralFloatExpression {
    type InputType = f64;
    type InputVector = LiteralValueVector<f64>;
    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error> {
        Ok(self.value)
    }
}

impl fmt::Display for LiteralFloatExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.value.get_value(0).unwrap())
    }
}

pub struct EqExpression<
    T,
    V: ColumnVector<DataType = T>,
    E: Expression<InputType = T, InputVector = V>,
> {
    left: E,
    right: E,
    phantom: PhantomData<V>,
}

impl<T, V: ColumnVector<DataType = T>, E: Expression<InputType = T, InputVector = V>> Expression
    for EqExpression<T, V, E>
{
    type InputType = T;
    type InputVector = V;
    fn evaluate<'a>(
        self,
        input: RecordBatch<Self::InputType, Self::InputVector>,
    ) -> Result<Self::OutputType, Error> {
        let l = self.left.evaluate(input)?;
        let r = self.right.evaluate(input)?;
        if l.size() == r.size() {
            Ok(l == r)
        } else {
            Err(Error::DifferentSizes(
                format!("{}", self.left),
                format!("{}", self.right),
            ))
        }
    }
}

impl<T, V: ColumnVector<DataType = T>, E: Expression<InputType = T, InputVector = V>>
    EqExpression<T, V, E>
{
    pub fn new(left: E, right: E) -> Self {
        EqExpression {
            left: left,
            right: right,
            phantom: PhantomData::default(),
        }
    }
}

impl<T, V: ColumnVector<DataType = T>, E: Expression<InputType = T, InputVector = V>> fmt::Display
    for EqExpression<T, V, E>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#Binary Expression")
    }
}
