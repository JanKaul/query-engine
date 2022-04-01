use arrow2::datatypes::Schema;

use crate::{
    data_source::ParquetDataSource,
    logical_plan::{
        logical_expression::LogicalExpression, Aggregate, LogicalPlan, Projection, Scan, Selection,
    },
};

pub trait DataFrameTrait {
    fn project(self, exprs: Vec<Box<dyn LogicalExpression>>) -> Self;

    fn filter(self, exprs: Box<dyn LogicalExpression>) -> Self;

    fn aggregate(
        self,
        group_by: Vec<Box<dyn LogicalExpression>>,
        aggregate_expr: Vec<Box<dyn LogicalExpression>>,
    ) -> Self;

    fn schema(&self) -> &Schema;

    fn logical_plan(self) -> Box<dyn LogicalPlan>;
}

pub struct DataFrame {
    plan: Box<dyn LogicalPlan>,
}

impl DataFrame {
    fn new(plan: Box<dyn LogicalPlan>) -> Self {
        DataFrame { plan: plan }
    }

    pub fn parquet(path: &str) -> Self {
        let ds = ParquetDataSource::new(path).unwrap();
        Self::new(Box::new(Scan::new(path, ds, None)))
    }
}

impl DataFrameTrait for DataFrame {
    fn project(self, exprs: Vec<Box<dyn LogicalExpression>>) -> Self {
        Self::new(Box::new(Projection::new(self.logical_plan(), exprs)))
    }

    fn filter(self, exprs: Box<dyn LogicalExpression>) -> Self {
        Self::new(Box::new(Selection::new(self.logical_plan(), exprs)))
    }

    fn aggregate(
        self,
        group_by: Vec<Box<dyn LogicalExpression>>,
        aggregate_expr: Vec<Box<dyn LogicalExpression>>,
    ) -> Self {
        Self::new(Box::new(Aggregate::new(
            self.logical_plan(),
            group_by,
            aggregate_expr,
        )))
    }

    fn schema(&self) -> &Schema {
        self.plan.schema().unwrap()
    }

    fn logical_plan(self) -> Box<dyn LogicalPlan> {
        self.plan
    }
}
