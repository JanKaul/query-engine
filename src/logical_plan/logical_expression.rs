use std::fmt;

use arrow2::datatypes;

use crate::{error::Error, schema::Field};

use super::LogicalPlan;

pub trait LogicalExpression: fmt::Display {
    fn toField<'a, T: LogicalPlan + ?Sized>(&self, input: &T) -> Result<Field, Error>;
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
    fn toField<'a, T: LogicalPlan + ?Sized>(&self, input: &T) -> Result<Field, Error> {
        input
            .schema()?
            .iter()
            .filter(|x| x.name == self.name)
            .next()
            .map(|x| x.clone())
            .ok_or(Error::NoFieldInLogicalPlan(self.name.clone()))
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.name)
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
    fn toField<'a, T: LogicalPlan + ?Sized>(&self, _input: &T) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.clone(),
            data_type: datatypes::DataType::Utf8,
        })
    }
}

impl fmt::Display for LiteralString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.value)
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
            fn toField<'a, T: LogicalPlan + ?Sized>(&self, _input: &T) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: datatypes::DataType::Boolean,
                })
            }
        }

        impl<L: LogicalExpression, R: LogicalExpression> fmt::Display for $i<L, R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {} {}", self.left, self.op, self.right)
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

// MathExpressions

macro_rules! mathExpression {
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
            fn toField<'a, T: LogicalPlan + ?Sized>(&self, input: &T) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.left.toField(input)?.data_type,
                })
            }
        }

        impl<L: LogicalExpression, R: LogicalExpression> fmt::Display for $i<L, R> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {} {}", self.left, self.op, self.right)
            }
        }
    };
}

mathExpression!(Add, "add".to_string(), "+".to_string());
mathExpression!(Sub, "sub".to_string(), "-".to_string());
mathExpression!(Mul, "mul".to_string(), "*".to_string());
mathExpression!(Div, "div".to_string(), "/".to_string());
mathExpression!(Mod, "mod".to_string(), "%".to_string());

// AggregateExpressions

macro_rules! aggregateExpression {
    ($i: ident, $name: expr) => {
        struct $i<T: LogicalExpression> {
            name: String,
            expr: T,
        }

        impl<T: LogicalExpression> $i<T> {
            pub fn new(expr: T) -> Self {
                $i {
                    name: $name,
                    expr: expr,
                }
            }
        }

        impl<T: LogicalExpression> LogicalExpression for $i<T> {
            fn toField<'a, I: LogicalPlan + ?Sized>(&self, input: &I) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.expr.toField(input)?.data_type,
                })
            }
        }

        impl<T: LogicalExpression> fmt::Display for $i<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} ({})", self.name, self.expr)
            }
        }
    };
}

aggregateExpression!(Sum, "sum".to_string());
aggregateExpression!(Avg, "avg".to_string());
aggregateExpression!(Max, "max".to_string());
aggregateExpression!(Min, "min".to_string());

// Count Expression

struct Count<T: LogicalExpression> {
    name: String,
    expr: T,
}

impl<T: LogicalExpression> Count<T> {
    pub fn new(expr: T) -> Self {
        Count {
            name: "count".to_string(),
            expr: expr,
        }
    }
}

impl<T: LogicalExpression> LogicalExpression for Count<T> {
    fn toField<'a, I: LogicalPlan + ?Sized>(&self, input: &I) -> Result<Field, Error> {
        Ok(Field {
            name: self.name.clone(),
            data_type: self.expr.toField(input)?.data_type,
        })
    }
}

impl<T: LogicalExpression> fmt::Display for Count<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.expr)
    }
}
