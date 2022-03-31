use std::fmt;

use arrow2::datatypes;

use crate::{error::Error, schema::Field};

use super::LogicalPlan;

pub trait LogicalExpression: fmt::Display {
    fn toField<'a, T: LogicalPlan>(&self, input: &T) -> Result<Field, Error>;
}

// Column expression
struct Column {
    name: String,
}

impl Column {
    fn new(name: String) -> Self {
        Column { name: name }
    }
}

impl LogicalExpression for Column {
    fn toField<'a, T: LogicalPlan>(&self, input: &T) -> Result<Field, Error> {
        input
            .schema()
            .iter()
            .filter(|x| x.name == self.name)
            .next()
            .map(|x| x.clone())
            .ok_or(Error::NoFieldInLogicalPlan(self.name.clone()))
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.name)
    }
}

// LiteralString expression

struct LiteralString {
    value: String,
}

impl LiteralString {
    fn new(value: String) -> Self {
        LiteralString { value: value }
    }
}

impl LogicalExpression for LiteralString {
    fn toField<'a, T: LogicalPlan>(&self, _input: &T) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.clone(),
            data_type: datatypes::DataType::Utf8,
        })
    }
}

impl fmt::Display for LiteralString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.value)
    }
}

// BinaryExpression expression

macro_rules! booleanBinaryExpression {
    ($i: ident, $name: expr, $op: expr) => {
        struct $i<L: LogicalExpression, R: LogicalExpression> {
            name: String,
            op: String,
            left: L,
            right: R,
        }

        impl<L: LogicalExpression, R: LogicalExpression> $i<L, R> {
            pub fn new(left: L, right: R) -> Self {
                $i {
                    name: $name,
                    op: $op,
                    left: left,
                    right: right,
                }
            }
        }

        impl<L: LogicalExpression, R: LogicalExpression> LogicalExpression for $i<L, R> {
            fn toField<'a, T: LogicalPlan>(&self, _input: &T) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: datatypes::DataType::Boolean,
                })
            }
        }

        impl<L: LogicalExpression, R: LogicalExpression> fmt::Display for $i<L, R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "({} {} {})", self.left, self.op, self.right)
            }
        }
    };
}

booleanBinaryExpression!(Eq, "eq".to_string(), "==".to_string());
booleanBinaryExpression!(Neq, "neq".to_string(), "!=".to_string());
booleanBinaryExpression!(Gt, "gt".to_string(), ">".to_string());
booleanBinaryExpression!(GtEq, "gteq".to_string(), ">=".to_string());
booleanBinaryExpression!(Lt, "lt".to_string(), "<".to_string());
booleanBinaryExpression!(LtEq, "lteq".to_string(), "<=".to_string());

// BooleanExpressions

booleanBinaryExpression!(And, "and".to_string(), "&&".to_string());
booleanBinaryExpression!(Or, "or".to_string(), "||".to_string());
