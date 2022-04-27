use arrow2::datatypes::Schema;

use crate::{
    data_source::{DataSource, ParquetDataSource},
    logical_plan::{
        logical_expression::LogicalExpression, Aggregate, LogicalPlan, Projection, Scan, Selection,
    },
};

pub trait DataFrameTrait {
    fn project(self, exprs: Vec<LogicalExpression>) -> Self;

    fn filter(self, exprs: LogicalExpression) -> Self;

    fn aggregate(
        self,
        group_by: Vec<LogicalExpression>,
        aggregate_expr: Vec<LogicalExpression>,
    ) -> Self;

    fn schema(&self) -> &Schema;

    fn logical_plan(self) -> LogicalPlan;
}

pub struct DataFrame {
    plan: LogicalPlan,
}

impl DataFrame {
    fn new(plan: LogicalPlan) -> Self {
        DataFrame { plan: plan }
    }

    pub fn parquet(path: &str) -> Self {
        let ds = DataSource::Parquet(ParquetDataSource::new(path).unwrap());
        Self::new(LogicalPlan::Scan(Scan::new(path, ds, None)))
    }
}

impl DataFrameTrait for DataFrame {
    fn project(self, exprs: Vec<LogicalExpression>) -> Self {
        Self::new(LogicalPlan::Projection(Projection::new(
            self.logical_plan(),
            exprs,
        )))
    }

    fn filter(self, exprs: LogicalExpression) -> Self {
        Self::new(LogicalPlan::Selection(Selection::new(
            self.logical_plan(),
            exprs,
        )))
    }

    fn aggregate(
        self,
        group_by: Vec<LogicalExpression>,
        aggregate_expr: Vec<LogicalExpression>,
    ) -> Self {
        Self::new(LogicalPlan::Aggregate(Aggregate::new(
            self.logical_plan(),
            group_by,
            aggregate_expr,
        )))
    }

    fn schema(&self) -> &Schema {
        self.plan.schema().unwrap()
    }

    fn logical_plan(self) -> LogicalPlan {
        self.plan
    }
}
