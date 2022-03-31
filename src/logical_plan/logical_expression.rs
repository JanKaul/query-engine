use std::fmt;

use arrow2::datatypes;

use crate::{error::Error, schema::Field};

use super::LogicalPlan;

pub trait LogicalExpression {
    fn toField<'a, T: LogicalPlan>(&self, input: &T) -> Result<Field, Error>;
}

// Column expression
struct Column {
    name: String,
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
