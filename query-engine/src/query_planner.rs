use arrow2::{
    datatypes::DataType,
    scalar::{BooleanScalar, PrimitiveScalar, Utf8Scalar},
};

use crate::{
    error::Error,
    logical_plan::{logical_expression::LogicalExpression, LogicalPlan},
    physical_plan::{
        physical_expressions::*, AggregateExec, PhysicalPlan, ProjectionExec, ScanExec,
        SelectionExec,
    },
};

impl LogicalExpression {
    pub(crate) fn to_physical_expression(
        self,
        input: &LogicalPlan,
    ) -> Result<Box<dyn PhysicalExpression>, Error> {
        match self {
            LogicalExpression::Column(col) => input
                .schema()?
                .fields
                .iter()
                .position(|x| x.name == col.name)
                .ok_or(Error::NoFieldInLogicalPlan(format!("{}", col)))
                .map(|index| Box::new(ColumnExpression { index }) as Box<dyn PhysicalExpression>),
            LogicalExpression::LiteralBool(bool) => Ok(Box::new(LiteralBoolExpression {
                value: BooleanScalar::new(Some(bool.value)),
            })
                as Box<dyn PhysicalExpression>),
            LogicalExpression::LiteralString(string) => Ok(Box::new(LiteralStringExpression {
                value: Utf8Scalar::new(Some(string.value)),
            })
                as Box<dyn PhysicalExpression>),
            LogicalExpression::LiteralInteger(int) => Ok(Box::new(LiteralIntegerExpression {
                value: PrimitiveScalar::new(DataType::Int32, Some(int.value)),
            })
                as Box<dyn PhysicalExpression>),
            LogicalExpression::LiteralFloat(float) => Ok(Box::new(LiteralFloatExpression {
                value: PrimitiveScalar::new(DataType::Float64, Some(float.value)),
            })
                as Box<dyn PhysicalExpression>),
            LogicalExpression::Eq(eq) => {
                let left = eq.left.to_physical_expression(input)?;
                let right = eq.right.to_physical_expression(input)?;
                Ok(Box::new(EqExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Neq(neq) => {
                let left = neq.left.to_physical_expression(input)?;
                let right = neq.right.to_physical_expression(input)?;
                Ok(Box::new(NeqExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Add(add) => {
                let left = add.left.to_physical_expression(input)?;
                let right = add.right.to_physical_expression(input)?;
                Ok(Box::new(AddExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Sub(sub) => {
                let left = sub.left.to_physical_expression(input)?;
                let right = sub.right.to_physical_expression(input)?;
                Ok(Box::new(SubExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Mul(mul) => {
                let left = mul.left.to_physical_expression(input)?;
                let right = mul.right.to_physical_expression(input)?;
                Ok(Box::new(MulExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Div(div) => {
                let left = div.left.to_physical_expression(input)?;
                let right = div.right.to_physical_expression(input)?;
                Ok(Box::new(DivExpression::new(left, right)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Max(max) => {
                let expr = max.expr.to_physical_expression(input)?;
                Ok(Box::new(MaxExpression::new(expr)) as Box<dyn PhysicalExpression>)
            }
            LogicalExpression::Min(min) => {
                let expr = min.expr.to_physical_expression(input)?;
                Ok(Box::new(MinExpression::new(expr)) as Box<dyn PhysicalExpression>)
            }
            e => Err(Error::PhysicalExpressionNotSuported(format!("{}", e))),
        }
    }

    pub(crate) fn to_physical_aggregate_expression(
        self,
        input: &LogicalPlan,
    ) -> Result<Box<dyn PhysicalAggregateExpression>, Error> {
        match self {
            LogicalExpression::Max(max) => {
                let expr = max.expr.to_physical_expression(input)?;
                Ok(Box::new(MaxExpression::new(expr)) as Box<dyn PhysicalAggregateExpression>)
            }
            LogicalExpression::Min(min) => {
                let expr = min.expr.to_physical_expression(input)?;
                Ok(Box::new(MinExpression::new(expr)) as Box<dyn PhysicalAggregateExpression>)
            }
            e => Err(Error::PhysicalExpressionNotSuported(format!("{}", e))),
        }
    }
}

impl LogicalPlan {
    pub(crate) fn to_physical_plan(self) -> Result<PhysicalPlan, Error> {
        match self {
            LogicalPlan::Scan(scan) => Ok(PhysicalPlan::Scan(ScanExec::new(
                scan.data_source,
                scan.projection,
                scan.schema,
            ))),
            LogicalPlan::Projection(proj) => {
                let input = &proj.children[0];
                let exprs = proj
                    .exprs
                    .into_iter()
                    .map(|x| x.to_physical_expression(input))
                    .collect::<Result<Vec<_>, _>>()?;
                let children = proj
                    .children
                    .into_iter()
                    .map(|x| x.to_physical_plan())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PhysicalPlan::Projection(ProjectionExec::new(
                    children,
                    exprs,
                    proj.schema,
                )))
            }
            LogicalPlan::Selection(sel) => {
                let expr = sel.expr.to_physical_expression(&sel.children[0])?;
                let children = sel
                    .children
                    .into_iter()
                    .map(|x| x.to_physical_plan())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PhysicalPlan::Selection(SelectionExec::new(
                    children, expr, sel.schema,
                )))
            }
            LogicalPlan::Aggregate(agg) => {
                let input = &agg.children[0];
                let group_exprs = agg
                    .group_exprs
                    .into_iter()
                    .map(|x| x.to_physical_expression(input))
                    .collect::<Result<Vec<_>, _>>()?;
                let agg_exprs = agg
                    .aggregate_exprs
                    .into_iter()
                    .map(|x| x.to_physical_aggregate_expression(input))
                    .collect::<Result<Vec<_>, _>>()?;
                let children = agg
                    .children
                    .into_iter()
                    .map(|x| x.to_physical_plan())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PhysicalPlan::Aggregate(AggregateExec::new(
                    children,
                    group_exprs,
                    agg_exprs,
                    agg.schema,
                )))
            }
        }
    }
}
