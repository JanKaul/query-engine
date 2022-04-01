use crate::logical_plan::logical_expression::{
    Column, LiteralFloat, LiteralInteger, LiteralString,
};

pub use crate::logical_plan::logical_expression::LogicalExpressionMethods;

pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

pub fn litString(name: &str) -> LiteralString {
    LiteralString::new(name.to_string())
}

pub fn litInt(value: i32) -> LiteralInteger {
    LiteralInteger::new(value)
}

pub fn litFloat(value: f64) -> LiteralFloat {
    LiteralFloat::new(value)
}
