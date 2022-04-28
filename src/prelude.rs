use crate::logical_plan::logical_expression::{
    Column, LiteralBool, LiteralFloat, LiteralInteger, LiteralString, LogicalExpression, Max, Min,
};

pub use crate::logical_plan::logical_expression::LogicalExpressionMethods;

pub fn col(name: &str) -> LogicalExpression {
    LogicalExpression::Column(Column::new(name.to_string()))
}

pub fn lit_string(name: &str) -> LogicalExpression {
    LogicalExpression::LiteralString(LiteralString::new(name.to_string()))
}

pub fn lit_int(value: i32) -> LogicalExpression {
    LogicalExpression::LiteralInteger(LiteralInteger::new(value))
}

pub fn lit_float(value: f64) -> LogicalExpression {
    LogicalExpression::LiteralFloat(LiteralFloat::new(value))
}

pub fn lit_bool(value: bool) -> LogicalExpression {
    LogicalExpression::LiteralBool(LiteralBool::new(value))
}

pub fn max(expr: LogicalExpression) -> LogicalExpression {
    LogicalExpression::Max(Box::new(Max::new(expr)))
}

pub fn min(expr: LogicalExpression) -> LogicalExpression {
    LogicalExpression::Min(Box::new(Min::new(expr)))
}
