use crate::data_source::DataSource;
use crate::error::Error;
use crate::schema::Schema;
use std::fmt;

mod logical_expression;
pub trait LogicalPlan: fmt::Display {
    fn schema(&self) -> Result<Schema, Error>;
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

fn format_logical_plan<T: LogicalPlan>(plan: &T, indent: usize) -> String {
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

struct Scan<D: DataSource> {
    path: String,
    data_source: D,
    projection: Option<Vec<String>>,
    schema: Schema,
}

impl<D: DataSource> Scan<D> {
    fn new(path: &str, data_source: D, projection: Option<Vec<String>>) -> Self {
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
                .iter()
                .filter(|x| pro.contains(&x.name))
                .map(|y| y.clone())
                .collect(),
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
    fn schema(&self) -> Result<Schema, Error> {
        Ok(self.schema.clone())
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        None
    }
}

struct Projection<E: logical_expression::LogicalExpression + fmt::Display> {
    exprs: Vec<E>,
    children: [Box<dyn LogicalPlan>; 1],
}

impl<E: logical_expression::LogicalExpression> Projection<E> {
    fn new(input: Box<dyn LogicalPlan>, exprs: Vec<E>) -> Self {
        Projection {
            exprs: exprs,
            children: [input],
        }
    }
}

impl<E: logical_expression::LogicalExpression + fmt::Display> fmt::Display for Projection<E> {
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
    fn schema(&self) -> Result<Schema, Error> {
        self.exprs
            .iter()
            .map(|expr| expr.toField(&*self.children[0]))
            .collect::<Result<Schema, Error>>()
    }
    fn children(&self) -> Option<&[Box<dyn LogicalPlan>]> {
        Some(&self.children)
    }
}
