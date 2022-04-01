use crate::data_source::DataSource;
use crate::error::Error;
use arrow2::datatypes::{Field, Schema};
use std::fmt;

mod logical_expression;
pub trait LogicalPlan: fmt::Display {
    fn schema(&self) -> Result<&Schema, Error>;
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]>;
}

fn format_children(plan: &Box<dyn LogicalPlan>, indent: usize) -> String {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str("\t"));
    result.push_str(&format!("{}", plan));
    result.push_str("\n");
    plan.children().map(|x| {
        x.iter()
            .for_each(|x| result.push_str(&format_children(&*x, indent + 1)));
        ()
    });
    result
}

pub fn format_logical_plan<T: LogicalPlan>(plan: &T, indent: usize) -> String {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str("\t"));
    result.push_str(&format!("{}", plan));
    result.push_str("\n");
    plan.children().map(|x| {
        x.iter()
            .for_each(|x| result.push_str(&format_children(&*x, indent + 1)));
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
struct Projection<E: logical_expression::LogicalExpression> {
    exprs: Vec<E>,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl<E: logical_expression::LogicalExpression> Projection<E> {
    fn new(input: Box<dyn LogicalPlan>, exprs: Vec<E>) -> Self {
        Projection {
            schema: Self::derive_schema(&exprs, &input),
            exprs: exprs,
            children: [input],
        }
    }

    fn derive_schema(exprs: &Vec<E>, input: &Box<dyn LogicalPlan>) -> Schema {
        exprs
            .iter()
            .map(|expr| expr.toField(&**input))
            .collect::<Result<Vec<Field>, Error>>()
            .map(|x| x.into())
            .unwrap()
    }
}

impl<E: logical_expression::LogicalExpression> fmt::Display for Projection<E> {
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

impl<E: logical_expression::LogicalExpression> LogicalPlan for Projection<E> {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}

// Selection

struct Selection<E: logical_expression::LogicalExpression + fmt::Display> {
    expr: E,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl<E: logical_expression::LogicalExpression> Selection<E> {
    fn new(input: Box<dyn LogicalPlan>, expr: E) -> Self {
        Selection {
            schema: Self::derive_schema(&expr, &input),
            expr: expr,
            children: [input],
        }
    }

    fn derive_schema(expr: &E, input: &Box<dyn LogicalPlan>) -> Schema {
        expr.toField(&**input)
            .map(|x| vec![x])
            .map(|x| x.into())
            .unwrap()
    }
}

impl<E: logical_expression::LogicalExpression + fmt::Display> fmt::Display for Selection<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Selection: {}", format!("{}, ", self.expr))
    }
}

impl<E: logical_expression::LogicalExpression> LogicalPlan for Selection<E> {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}

// Aggregate
struct Aggregate<E: logical_expression::LogicalExpression + fmt::Display> {
    group_exprs: Vec<E>,
    aggregate_exprs: Vec<E>,
    children: [Box<dyn LogicalPlan>; 1],
    schema: Schema,
}

impl<E: logical_expression::LogicalExpression> Aggregate<E> {
    fn new(input: Box<dyn LogicalPlan>, group_exprs: Vec<E>, aggregate_exprs: Vec<E>) -> Self {
        Aggregate {
            schema: Self::derive_schema(&group_exprs, &aggregate_exprs, &input),
            group_exprs: group_exprs,
            aggregate_exprs: aggregate_exprs,
            children: [input],
        }
    }

    fn derive_schema(
        group_exprs: &Vec<E>,
        aggregate_exprs: &Vec<E>,
        input: &Box<dyn LogicalPlan>,
    ) -> Schema {
        group_exprs
            .iter()
            .chain(aggregate_exprs.iter())
            .map(|expr| expr.toField(&**input))
            .collect::<Result<Vec<Field>, Error>>()
            .map(|x| x.into())
            .unwrap()
    }
}

impl<E: logical_expression::LogicalExpression + fmt::Display> fmt::Display for Aggregate<E> {
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

impl<E: logical_expression::LogicalExpression> LogicalPlan for Aggregate<E> {
    fn schema(&self) -> Result<&Schema, Error> {
        Ok(&self.schema)
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}
