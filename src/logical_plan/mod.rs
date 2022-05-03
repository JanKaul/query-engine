use crate::data_source::DataSource;
use crate::error::Error;
use arrow2::datatypes::{Field, Schema};
use std::fmt;

use self::logical_expression::LogicalExpression;

pub mod logical_expression;
pub mod optimizer;

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
    fn children(&self) -> Option<&[LogicalPlan]> {
        match self {
            LogicalPlan::Scan(scan) => scan.children(),
            LogicalPlan::Projection(proj) => proj.children(),
            LogicalPlan::Selection(sel) => sel.children(),
            LogicalPlan::Aggregate(agg) => agg.children(),
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalPlan::Scan(scan) => write!(f, "{}", scan),
            LogicalPlan::Projection(proj) => write!(f, "{}", proj),
            LogicalPlan::Selection(sel) => write!(f, "{}", sel),
            LogicalPlan::Aggregate(agg) => write!(f, "{}", agg),
        }
    }
}

pub fn format_logical_plan(plan: &LogicalPlan, indent: usize) -> String {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str(" \t"));
    result.push_str(&format!("{}", plan));
    result.push_str(" \n");
    plan.children().map(|x| {
        x.iter()
            .for_each(|child| result.push_str(&format_logical_plan(child, indent + 1)))
    });
    result
}

// Scan logical plan

pub struct Scan {
    pub(crate) path: String,
    pub(crate) data_source: DataSource,
    pub(crate) projection: Option<Vec<String>>,
    pub(crate) schema: Schema,
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
    #[inline]
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    #[inline]
    fn children(&self) -> Option<&[LogicalPlan]> {
        None
    }
}

// Projection
pub struct Projection {
    pub(crate) exprs: Vec<LogicalExpression>,
    pub(crate) children: Vec<LogicalPlan>,
    pub(crate) schema: Schema,
}

impl Projection {
    pub fn new(input: LogicalPlan, exprs: Vec<LogicalExpression>) -> Self {
        Projection {
            schema: Self::derive_schema(&exprs, &input),
            exprs: exprs,
            children: vec![input],
        }
    }

    fn derive_schema(exprs: &Vec<LogicalExpression>, input: &LogicalPlan) -> Schema {
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
    #[inline]
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    #[inline]
    fn children(&self) -> Option<&[LogicalPlan]> {
        Some(&self.children)
    }
}

// Selection

pub struct Selection {
    pub(crate) expr: LogicalExpression,
    pub(crate) children: Vec<LogicalPlan>,
    pub(crate) schema: Schema,
}

impl Selection {
    pub fn new(input: LogicalPlan, expr: LogicalExpression) -> Self {
        Selection {
            schema: Self::derive_schema(&expr, &input),
            expr: expr,
            children: vec![input],
        }
    }

    fn derive_schema(expr: &LogicalExpression, input: &LogicalPlan) -> Schema {
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
    #[inline]
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    #[inline]
    fn children(&self) -> Option<&[LogicalPlan]> {
        Some(&self.children)
    }
}

// Aggregate
pub struct Aggregate {
    pub(crate) group_exprs: Vec<LogicalExpression>,
    pub(crate) aggregate_exprs: Vec<LogicalExpression>,
    pub(crate) children: Vec<LogicalPlan>,
    pub(crate) schema: Schema,
}

impl Aggregate {
    pub fn new(
        input: LogicalPlan,
        group_exprs: Vec<LogicalExpression>,
        aggregate_exprs: Vec<LogicalExpression>,
    ) -> Self {
        Aggregate {
            schema: Self::derive_schema(&group_exprs, &aggregate_exprs, &input),
            group_exprs: group_exprs,
            aggregate_exprs: aggregate_exprs,
            children: vec![input],
        }
    }

    fn derive_schema(
        group_exprs: &Vec<LogicalExpression>,
        aggregate_exprs: &Vec<LogicalExpression>,
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
    #[inline]
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    #[inline]
    fn children(&self) -> Option<&[LogicalPlan]> {
        Some(&self.children)
    }
}
