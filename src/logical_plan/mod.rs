use crate::data_source::DataSource;
use crate::error::Error;
use arrow2::datatypes::{Field, Schema};
use std::fmt;

use self::logical_expression::LogicalExpression;

pub mod logical_expression;

pub enum LogicalPlan {
    Scan(Scan),
    Projection(Projection),
    Selection(Selection),
    Aggregate(Aggregate),
}

impl LogicalPlan {
    pub fn schema(&self) -> Result<&Schema, Error> {
        match self {
            LogicalPlan::Scan(scan) => scan.schema(),
            LogicalPlan::Projection(proj) => proj.schema(),
            LogicalPlan::Selection(sel) => sel.schema(),
            LogicalPlan::Aggregate(agg) => agg.schema(),
        }
    }
}

pub fn format_logical_plan(plan: &LogicalPlan, indent: usize) -> String {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str(" \t"));
    match plan {
        LogicalPlan::Scan(scan) => {
            result.push_str(&format!("{}", scan));
            result.push_str(" \n");
            scan.children()
                .map(|child| result.push_str(&format_logical_plan(child, indent + 1)));
        }
        LogicalPlan::Projection(proj) => {
            result.push_str(&format!("{}", proj));
            result.push_str(" \n");
            result.push_str(&format_logical_plan(proj.children(), indent + 1));
        }
        LogicalPlan::Selection(sel) => {
            result.push_str(&format!("{}", sel));
            result.push_str(" \n");
            result.push_str(&format_logical_plan(sel.children(), indent + 1));
        }
        LogicalPlan::Aggregate(agg) => {
            result.push_str(&format!("{}", agg));
            result.push_str(" \n");
            result.push_str(&format_logical_plan(agg.children(), indent + 1));
        }
    }
    result
}

// Scan logical plan

pub struct Scan {
    path: String,
    data_source: DataSource,
    projection: Option<Vec<String>>,
    schema: Schema,
}

impl Scan {
    pub fn new(path: &str, data_source: DataSource, projection: Option<Vec<String>>) -> Self {
        Scan {
            path: path.to_string(),
            schema: Self::derive_schema(&data_source, &projection),
            data_source: data_source,
            projection: projection,
        }
    }

    fn derive_schema(data_source: &DataSource, projection: &Option<Vec<String>>) -> Schema {
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

impl fmt::Display for Scan {
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

impl Scan {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&LogicalPlan> {
        None
    }
}

// Projection
pub struct Projection {
    exprs: Vec<Box<dyn LogicalExpression>>,
    children: Box<LogicalPlan>,
    schema: Schema,
}

impl Projection {
    pub fn new(input: LogicalPlan, exprs: Vec<Box<dyn LogicalExpression>>) -> Self {
        Projection {
            schema: Self::derive_schema(&exprs, &input),
            exprs: exprs,
            children: Box::new(input),
        }
    }

    fn derive_schema(exprs: &Vec<Box<dyn LogicalExpression>>, input: &LogicalPlan) -> Schema {
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

impl Projection {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> &LogicalPlan {
        &self.children
    }
}

// Selection

pub struct Selection {
    expr: Box<dyn LogicalExpression>,
    children: Box<LogicalPlan>,
    schema: Schema,
}

impl Selection {
    pub fn new(input: LogicalPlan, expr: Box<dyn LogicalExpression>) -> Self {
        Selection {
            schema: Self::derive_schema(&expr, &input),
            expr: expr,
            children: Box::new(input),
        }
    }

    fn derive_schema(expr: &Box<dyn LogicalExpression>, input: &LogicalPlan) -> Schema {
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

impl Selection {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> &LogicalPlan {
        &self.children
    }
}

// Aggregate
pub struct Aggregate {
    group_exprs: Vec<Box<dyn LogicalExpression>>,
    aggregate_exprs: Vec<Box<dyn LogicalExpression>>,
    children: Box<LogicalPlan>,
    schema: Schema,
}

impl Aggregate {
    pub fn new(
        input: LogicalPlan,
        group_exprs: Vec<Box<dyn LogicalExpression>>,
        aggregate_exprs: Vec<Box<dyn LogicalExpression>>,
    ) -> Self {
        Aggregate {
            schema: Self::derive_schema(&group_exprs, &aggregate_exprs, &input),
            group_exprs: group_exprs,
            aggregate_exprs: aggregate_exprs,
            children: Box::new(input),
        }
    }

    fn derive_schema(
        group_exprs: &Vec<Box<dyn LogicalExpression>>,
        aggregate_exprs: &Vec<Box<dyn LogicalExpression>>,
        input: &LogicalPlan,
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

impl Aggregate {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> &LogicalPlan {
        &self.children
    }
}
