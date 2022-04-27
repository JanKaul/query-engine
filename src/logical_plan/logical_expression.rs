use std::fmt::{self, Display};

use arrow2::{
    datatypes,
    datatypes::{Field, Metadata},
};

use crate::error::Error;

use super::LogicalPlan;

pub enum LogicalExpression {
    Column(Column),
    LiteralBool(LiteralBool),
    LiteralString(LiteralString),
    LiteralInteger(LiteralInteger),
    LiteralFloat(LiteralFloat),
    Eq(Box<Eq>),
    Neq(Box<Neq>),
    Gt(Box<Gt>),
    GtEq(Box<GtEq>),
    Lt(Box<Lt>),
    LtEq(Box<LtEq>),
    And(Box<And>),
    Or(Box<Or>),
    Add(Box<Add>),
    Sub(Box<Sub>),
    Mul(Box<Mul>),
    Div(Box<Div>),
    Mod(Box<Mod>),
    Sum(Box<Sum>),
    Avg(Box<Avg>),
    Max(Box<Max>),
    Min(Box<Min>),
    Count(Box<Count>)
}

impl LogicalExpression {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
        match self {
            LogicalExpression::Column(col) => col.to_field(input),
            LogicalExpression::LiteralBool(bool) => bool.to_field(input),
            LogicalExpression::LiteralString(string) => string.to_field(input),
            LogicalExpression::LiteralInteger(int) => int.to_field(input),
            LogicalExpression::LiteralFloat(float) => float.to_field(input),
            LogicalExpression::Eq(eq)=> eq.to_field(input),
            LogicalExpression::Neq(neq)=> neq.to_field(input),
            LogicalExpression::Gt(gt)=> gt.to_field(input),
            LogicalExpression::GtEq(gteq)=> gteq.to_field(input),
            LogicalExpression::Lt(lt)=> lt.to_field(input),
            LogicalExpression::LtEq(lteq)=> lteq.to_field(input),
            LogicalExpression::And(and) => and.to_field(input),
            LogicalExpression::Or(or) => or.to_field(input),
            LogicalExpression::Add(add) => add.to_field(input),
            LogicalExpression::Sub(sub) => sub.to_field(input),
            LogicalExpression::Mul(mul) => mul.to_field(input),
            LogicalExpression::Div(div) => div.to_field(input),
            LogicalExpression::Mod(modu) => modu.to_field(input),
            LogicalExpression::Sum(sum) => sum.to_field(input),
            LogicalExpression::Avg(avg) => avg.to_field(input),
            LogicalExpression::Max(max) => max.to_field(input),
            LogicalExpression::Min(min) => min.to_field(input),
            LogicalExpression::Count(count) => count.to_field(input)
        }
    }
}

impl Display for LogicalExpression {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                LogicalExpression::Column(col) => write!(f, "{}", col),
            LogicalExpression::LiteralBool(bool) => write!(f, "{}", bool),
            LogicalExpression::LiteralString(string) => write!(f, "{}", string),
            LogicalExpression::LiteralInteger(int) => write!(f, "{}", int),
            LogicalExpression::LiteralFloat(float) => write!(f, "{}", float),
            LogicalExpression::Eq(eq)=> write!(f, "{}", eq),
            LogicalExpression::Neq(neq)=> write!(f, "{}", neq),
            LogicalExpression::Gt(gt)=> write!(f, "{}", gt),
            LogicalExpression::GtEq(gteq)=> write!(f, "{}", gteq),
            LogicalExpression::Lt(lt)=> write!(f, "{}", lt),
            LogicalExpression::LtEq(lteq)=> write!(f, "{}", lteq),
            LogicalExpression::And(and) => write!(f, "{}", and),
            LogicalExpression::Or(or) => write!(f, "{}", or),
            LogicalExpression::Add(add) => write!(f, "{}", add),
            LogicalExpression::Sub(sub) => write!(f, "{}", sub),
            LogicalExpression::Mul(mul) => write!(f, "{}", mul),
            LogicalExpression::Div(div) => write!(f, "{}", div),
            LogicalExpression::Mod(modu) => write!(f, "{}", modu),
            LogicalExpression::Sum(sum) => write!(f, "{}", sum),
            LogicalExpression::Avg(avg) => write!(f, "{}", avg),
            LogicalExpression::Max(max) => write!(f, "{}", max),
            LogicalExpression::Min(min) => write!(f, "{}", min),
            LogicalExpression::Count(count) => write!(f, "{}", count)
            }
        }
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

impl Column {
    #[inline]
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

pub struct LiteralBool {
    value: bool,
}

impl LiteralBool {
    pub fn new(value: bool) -> Self {
        LiteralBool { value: value }
    }
}

impl  LiteralBool {
    #[inline]
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.to_string(),
            data_type: datatypes::DataType::Boolean,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl fmt::Display for LiteralBool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.value)
    }
}
pub struct LiteralString {
    value: String,
}

impl LiteralString {
    pub fn new(value: String) -> Self {
        LiteralString { value: value }
    }
}

impl  LiteralString {
    #[inline]
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

impl  LiteralInteger {
    #[inline]
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

impl  LiteralFloat {
    #[inline]
    fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.value.to_string(),
            data_type: datatypes::DataType::Float64,
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
        pub struct $i {
            name: String,
            op: String,
            left: LogicalExpression,
            right: LogicalExpression,
        }

        impl $i {
            pub fn new(left: LogicalExpression, right: LogicalExpression) -> Self {
                $i {
                    name: $name,
                    op: $op,
                    left: left,
                    right: right,
                }
            }
        }

        impl  $i {
            #[inline]
            fn to_field(&self, _input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: datatypes::DataType::Boolean,
                    is_nullable: false,
                    metadata: Metadata::default(),
                })
            }
        }

        impl fmt::Display for $i {
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
        pub struct $i {
            name: String,
            op: String,
            left: LogicalExpression,
            right: LogicalExpression,
        }

        impl $i {
            pub fn new(left: LogicalExpression, right: LogicalExpression) -> Self {
                $i {
                    name: $name,
                    op: $op,
                    left: left,
                    right: right,
                }
            }
        }

        impl  $i {
            #[inline]
            fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.left.to_field(input)?.data_type,
                    is_nullable: false,
                    metadata: Metadata::default(),
                })
            }
        }

        impl fmt::Display for $i {
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

pub trait LogicalAggregateExpression {}

macro_rules! aggregateExpression {
    ($i: ident, $name: expr) => {
        pub struct $i {
            name: String,
            expr: LogicalExpression,
        }

        impl $i {
            pub fn new(expr: LogicalExpression) -> Self {
                $i {
                    name: $name,
                    expr: expr,
                }
            }
        }

        impl  $i {
            #[inline]
            fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
                Ok(Field {
                    name: self.name.clone(),
                    data_type: self.expr.to_field(input)?.data_type,
                    is_nullable: false,
                    metadata: Metadata::default(),
                })
            }
        }

        impl LogicalAggregateExpression for $i {}

        impl fmt::Display for $i {
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

pub struct Count {
    name: String,
    expr: LogicalExpression,
}

impl Count {
    pub fn new(expr: LogicalExpression) -> Self {
        Count {
            name: "count".to_string(),
            expr: expr,
        }
    }
}

impl  Count {
    #[inline]
    fn to_field(&self, input: &LogicalPlan) -> Result<Field, Error> {
        Ok(Field {
            name: self.name.clone(),
            data_type: self.expr.to_field(input)?.data_type,
            is_nullable: false,
            metadata: Metadata::default(),
        })
    }
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.expr)
    }
}

pub trait LogicalExpressionMethods {
    fn eq(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn neq(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn gt(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn gteq(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn lt(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn lteq(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn and(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
    fn or(self, other: LogicalExpression) -> LogicalExpression
    where
        Self: Sized;
}

macro_rules! booleanMethod {
    ($name: ident, $t: ident) => {
        fn $name(self, other: LogicalExpression) -> LogicalExpression
        where
            Self: Sized,
        {
            LogicalExpression::$t(Box::new($t::new(self, other)))
        }
    };
}

impl LogicalExpressionMethods for LogicalExpression {
    booleanMethod!(eq, Eq);
    booleanMethod!(neq, Neq);
    booleanMethod!(gt, Gt);
    booleanMethod!(gteq, GtEq);
    booleanMethod!(lt, Lt);
    booleanMethod!(lteq, LtEq);
    booleanMethod!(and, And);
    booleanMethod!(or, Or);
}
