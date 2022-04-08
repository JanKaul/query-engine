use crate::logical_plan::logical_expression::{
    Column, LiteralBool, LiteralFloat, LiteralInteger, LiteralString,
};

pub use crate::logical_plan::logical_expression::LogicalExpressionMethods;

pub fn col(name: &str) -> Column {
    Column::new(name.to_string())
}

pub fn lit_string(name: &str) -> LiteralString {
    LiteralString::new(name.to_string())
}

pub fn lit_int(value: i32) -> LiteralInteger {
    LiteralInteger::new(value)
}

pub fn lit_float(value: f64) -> LiteralFloat {
    LiteralFloat::new(value)
}

pub fn lit_bool(value: bool) -> LiteralBool {
    LiteralBool::new(value)
}
