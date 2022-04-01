use crate::data_source::DataSource;
use crate::error::Error;
use arrow2::datatypes::{Field, Schema};
use std::fmt;

use self::logical_expression::LogicalExpression;

pub mod logical_expression;
pub trait LogicalPlan: fmt::Display {
    fn schema(&self) -> Result<&Schema, Error>;
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]>;
}

pub fn format_logical_plan(plan: &Box<dyn LogicalPlan>, indent: usize) -> String {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str(" \t"));
    result.push_str(&format!("{}", plan));
    result.push_str(" \n");
    plan.children().map(|x| {
        x.iter()
            .for_each(|x| result.push_str(&format_logical_plan(&*x, indent + 1)));
        ()
    });
    result
}

// Scan logical plan

pub struct Scan<D: DataSource> {
    path: String,
    data_source: D,
    projection: Option<Vec<String>>,
    schema: Schema,
}

impl<D: DataSource> Scan<D> {
    pub fn new(path: &str, data_source: D, projection: Option<Vec<String>>) -> Self {
        Scan {
            path: path.to_string(),
            schema: Self::derive_schema(&data_source, &projection),
            data_source: data_source,
            projection: projection,
        }
    }

    fn derive_schema(data_source: &D, projection: &Option<Vec<String>>) -> Schema {
        match projection {
            Some(pro) => data_source
                .schema()
                .fields
                .iter()
                .filter(|x| pro.contains(&x.name))
                .map(|y| y.clone())
                .collect::<Vec<Field>>()
                .into(),
            None => data_source.schema(),
        }
    }
}

impl<D: DataSource> fmt::Display for Scan<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.projection {
            Some(proj) => write!(
                f,
                "Scan: {}; projection={}",
                self.path,
                String::from_iter(proj.clone().into_iter())
            ),
            None => write!(f, "Scan: {}; projection=None", self.path),
        }
    }
}

impl<D: DataSource> LogicalPlan for Scan<D> {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        None
    }
}

// Projection
pub struct Projection {
    exprs: Vec<Box<dyn LogicalExpression>>,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl Projection {
    pub fn new(input: Box<dyn LogicalPlan>, exprs: Vec<Box<dyn LogicalExpression>>) -> Self {
        Projection {
            schema: Self::derive_schema(&exprs, &input),
            exprs: exprs,
            children: [input],
        }
    }

    fn derive_schema(
        exprs: &Vec<Box<dyn LogicalExpression>>,
        input: &Box<dyn LogicalPlan>,
    ) -> Schema {
        exprs
            .iter()
            .map(|expr| expr.to_field(input))
            .collect::<Result<Vec<Field>, Error>>()
            .map(|x| x.into())
            .unwrap()
    }
}

impl fmt::Display for Projection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Projection: {}",
            self.exprs
                .iter()
                .map(|expr| format!("{}, ", expr))
                .collect::<String>()
        )
    }
}

impl LogicalPlan for Projection {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}

// Selection

pub struct Selection {
    expr: Box<dyn LogicalExpression>,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl Selection {
    pub fn new(input: Box<dyn LogicalPlan>, expr: Box<dyn LogicalExpression>) -> Self {
        Selection {
            schema: Self::derive_schema(&expr, &input),
            expr: expr,
            children: [input],
        }
    }

    fn derive_schema(expr: &Box<dyn LogicalExpression>, input: &Box<dyn LogicalPlan>) -> Schema {
        expr.to_field(input)
            .map(|x| vec![x])
            .map(|x| x.into())
            .unwrap()
    }
}

impl fmt::Display for Selection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Selection: {}", format!("{}, ", self.expr))
    }
}

impl LogicalPlan for Selection {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}

// Aggregate
pub struct Aggregate {
    group_exprs: Vec<Box<dyn LogicalExpression>>,
    aggregate_exprs: Vec<Box<dyn LogicalExpression>>,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl Aggregate {
    pub fn new(
        input: Box<dyn LogicalPlan>,
        group_exprs: Vec<Box<dyn LogicalExpression>>,
        aggregate_exprs: Vec<Box<dyn LogicalExpression>>,
    ) -> Self {
        Aggregate {
            schema: Self::derive_schema(&group_exprs, &aggregate_exprs, &input),
            group_exprs: group_exprs,
            aggregate_exprs: aggregate_exprs,
            children: [input],
        }
    }

    fn derive_schema(
        group_exprs: &Vec<Box<dyn LogicalExpression>>,
        aggregate_exprs: &Vec<Box<dyn LogicalExpression>>,
        input: &Box<dyn LogicalPlan>,
    ) -> Schema {
        group_exprs
            .iter()
            .chain(aggregate_exprs.iter())
            .map(|expr| expr.to_field(input))
            .collect::<Result<Vec<Field>, Error>>()
            .map(|x| x.into())
            .unwrap()
    }
}

impl fmt::Display for Aggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Aggregate: {}",
            self.group_exprs
                .iter()
                .chain(self.aggregate_exprs.iter())
                .map(|expr| format!("{}, ", expr))
                .collect::<String>()
        )
    }
}

impl LogicalPlan for Aggregate {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}
