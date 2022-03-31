use crate::schema::Schema;
use std::fmt::Display;

mod logical_expression;
pub trait LogicalPlan: Display {
    fn schema(&self) -> &Schema;
    fn children(&self) -> &[Box<dyn LogicalPlan>];
}

fn format_logical_plan(plan: &Box<dyn LogicalPlan>, indent: usize) {
    let mut result = String::new();
    (0..indent).for_each(|_| result.push_str("\t"));
    result.push_str(&format!("{}", plan));
    result.push_str("\n");
    plan.children()
        .iter()
        .for_each(|x| format_logical_plan(&*x, indent + 1))
}
