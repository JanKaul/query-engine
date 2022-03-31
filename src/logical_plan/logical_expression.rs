use std::fmt;

use crate::{error::Error, schema::Field};

use super::LogicalPlan;

pub trait LogicalExpression {
    fn toField<'a, T: LogicalPlan>(&self, input: &'a T) -> Result<&'a Field, Error>;
}

struct Column {
    name: String,
}

impl LogicalExpression for Column {
    fn toField<'a, T: LogicalPlan>(&self, input: &'a T) -> Result<&'a Field, Error> {
        input
            .schema()
            .iter()
            .filter(|x| x.name == self.name)
            .next()
            .ok_or(Error::ExceedingBoundsError(0))
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.name)
    }
}
