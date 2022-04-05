use std::fmt;

use arrow2::{
    datatypes,
    datatypes::{Field, Metadata},
};

use crate::error::Error;

use super::LogicalPlan;

pub trait LogicalExpression: fmt::Display {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error>;
}

// Column expression
pub struct Column {
    name: String,
}

impl Column {
    pub fn new(name: String) -> Self {
        Column { name: name }
    }
}

impl LogicalExpression for Column {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
        input
            .schema()?
            .fields
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

pub struct LiteralString {
    value: String,
}

impl LiteralString {
    pub fn new(value: String) -> Self {
        LiteralString { value: value }
    }
}

impl LogicalExpression for LiteralString {
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.clone(),
            data_type: datatypes::DataType::Utf8,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl fmt::Display for LiteralString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.value)
    }
}

pub struct LiteralInteger {
    value: i32,
}

impl LiteralInteger {
    pub fn new(value: i32) -> Self {
        LiteralInteger { value: value }
    }
}

impl LogicalExpression for LiteralInteger {
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.to_string(),
            data_type: datatypes::DataType::Int32,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl fmt::Display for LiteralInteger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.value)
    }
}

pub struct LiteralFloat {
    value: f64,
}

impl LiteralFloat {
    pub fn new(value: f64) -> Self {
        LiteralFloat { value: value }
    }
}

impl LogicalExpression for LiteralFloat {
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.to_string(),
            data_type: datatypes::DataType::Utf8,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl fmt::Display for LiteralFloat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.value)
    }
}

// BinaryExpression expression

macro_rules! booleanBinaryExpression {
    ($i: ident, $name: expr, $op: expr) => {
        pub struct $i<L: LogicalExpression, R: LogicalExpression> {
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
            fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: datatypes::DataType::Boolean,
                    is_nullable: false,
                    metadata: Metadata::default(),
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
        pub struct $i<L: LogicalExpression, R: LogicalExpression> {
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
            fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.left.to_field(input)?.data_type,
                    is_nullable: false,
                    metadata: Metadata::default(),
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

pub trait AggregateExpression: LogicalExpression {}

macro_rules! aggregateExpression {
    ($i: ident, $name: expr) => {
        pub struct $i<T: LogicalExpression> {
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
            fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.expr.to_field(input)?.data_type,
                    is_nullable: false,
                    metadata: Metadata::default(),
                })
            }
        }

        impl<T: LogicalExpression> AggregateExpression for $i<T> {}

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

pub struct Count<T: LogicalExpression> {
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
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.name.clone(),
            data_type: self.expr.to_field(input)?.data_type,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl<T: LogicalExpression> fmt::Display for Count<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.expr)
    }
}

pub trait LogicalExpressionMethods: LogicalExpression {
    fn eq<T: LogicalExpression>(self, other: T) -> Eq<Self, T>
    where
        Self: Sized;
    fn neq<T: LogicalExpression>(self, other: T) -> Neq<Self, T>
    where
        Self: Sized;
    fn gt<T: LogicalExpression>(self, other: T) -> Gt<Self, T>
    where
        Self: Sized;
    fn gteq<T: LogicalExpression>(self, other: T) -> GtEq<Self, T>
    where
        Self: Sized;
    fn lt<T: LogicalExpression>(self, other: T) -> Lt<Self, T>
    where
        Self: Sized;
    fn lteq<T: LogicalExpression>(self, other: T) -> LtEq<Self, T>
    where
        Self: Sized;
    fn and<T: LogicalExpression>(self, other: T) -> And<Self, T>
    where
        Self: Sized;
    fn or<T: LogicalExpression>(self, other: T) -> Or<Self, T>
    where
        Self: Sized;
}

macro_rules! booleanMethod {
    ($name: ident, $t: ident) => {
        fn $name<O: LogicalExpression>(self, other: O) -> $t<Self, O>
        where
            Self: Sized,
        {
            $t::new(self, other)
        }
    };
}

impl<T: LogicalExpression> LogicalExpressionMethods for T {
    booleanMethod!(eq, Eq);
    booleanMethod!(neq, Neq);
    booleanMethod!(gt, Gt);
    booleanMethod!(gteq, GtEq);
    booleanMethod!(lt, Lt);
    booleanMethod!(lteq, LtEq);
    booleanMethod!(and, And);
    booleanMethod!(or, Or);
}
