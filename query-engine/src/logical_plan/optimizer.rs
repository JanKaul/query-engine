use std::collections::HashSet;

use super::{
    logical_expression::LogicalExpression, Aggregate, LogicalPlan, Projection, Scan, Selection,
};

impl LogicalPlan {
    pub fn optimize(self) -> Self {
        self.projection_push_down()
    }
}

// Projection push down

impl LogicalPlan {
    fn projection_push_down(self) -> Self {
        let mut hash_set = HashSet::new();
        self.push_down(&mut hash_set)
    }
    fn push_down(self, hash_set: &mut HashSet<String>) -> Self {
        match self {
            LogicalPlan::Scan(scan) => {
                let hash_set = match scan.projection {
                    Some(projs) => projs.into_iter().fold(hash_set, |hash_set, x| {
                        hash_set.insert(x);
                        hash_set
                    }),
                    None => hash_set,
                };
                LogicalPlan::Scan(Scan::new(
                    &scan.path,
                    scan.data_source,
                    Some(hash_set.iter().map(|x| x.clone()).collect::<Vec<String>>()),
                ))
            }
            LogicalPlan::Aggregate(mut agg) => {
                let input = agg.children.pop().unwrap();
                extract_all_columns(&agg.group_exprs, &input, hash_set);
                extract_all_columns(&agg.aggregate_exprs, &input, hash_set);
                LogicalPlan::Aggregate(Aggregate::new(
                    input.push_down(hash_set),
                    agg.group_exprs,
                    agg.aggregate_exprs,
                ))
            }
            LogicalPlan::Projection(mut proj) => {
                let input = proj.children.pop().unwrap();
                extract_all_columns(&proj.exprs, &input, hash_set);
                LogicalPlan::Projection(Projection::new(input.push_down(hash_set), proj.exprs))
            }
            LogicalPlan::Selection(mut sel) => {
                let input = sel.children.pop().unwrap();
                extract_columns(&sel.expr, &input, hash_set);
                LogicalPlan::Selection(Selection::new(input.push_down(hash_set), sel.expr))
            }
        }
    }
}

fn extract_all_columns(
    exprs: &[LogicalExpression],
    plan: &LogicalPlan,
    hash_set: &mut HashSet<String>,
) {
    exprs.iter().fold(hash_set, |hash_set, expr| {
        extract_columns(expr, plan, hash_set);
        hash_set
    });
}

fn extract_columns(expr: &LogicalExpression, plan: &LogicalPlan, hash_set: &mut HashSet<String>) {
    match expr {
        LogicalExpression::Column(column) => {
            hash_set.insert(column.name.clone());
        }
        LogicalExpression::LiteralBool(_) => {}
        LogicalExpression::LiteralString(_) => {}
        LogicalExpression::LiteralInteger(_) => {}
        LogicalExpression::LiteralFloat(_) => {}
        LogicalExpression::Eq(eq) => {
            extract_columns(&eq.left, plan, hash_set);
            extract_columns(&eq.right, plan, hash_set)
        }
        LogicalExpression::Neq(neq) => {
            extract_columns(&neq.left, plan, hash_set);
            extract_columns(&neq.right, plan, hash_set)
        }
        LogicalExpression::Gt(gt) => {
            extract_columns(&gt.left, plan, hash_set);
            extract_columns(&gt.right, plan, hash_set)
        }
        LogicalExpression::GtEq(gteq) => {
            extract_columns(&gteq.left, plan, hash_set);
            extract_columns(&gteq.right, plan, hash_set)
        }
        LogicalExpression::Lt(lt) => {
            extract_columns(&lt.left, plan, hash_set);
            extract_columns(&lt.right, plan, hash_set)
        }
        LogicalExpression::LtEq(lteq) => {
            extract_columns(&lteq.left, plan, hash_set);
            extract_columns(&lteq.right, plan, hash_set)
        }
        LogicalExpression::And(and) => {
            extract_columns(&and.left, plan, hash_set);
            extract_columns(&and.right, plan, hash_set)
        }
        LogicalExpression::Or(or) => {
            extract_columns(&or.left, plan, hash_set);
            extract_columns(&or.right, plan, hash_set)
        }
        LogicalExpression::Add(add) => {
            extract_columns(&add.left, plan, hash_set);
            extract_columns(&add.right, plan, hash_set)
        }
        LogicalExpression::Sub(sub) => {
            extract_columns(&sub.left, plan, hash_set);
            extract_columns(&sub.right, plan, hash_set)
        }
        LogicalExpression::Div(div) => {
            extract_columns(&div.left, plan, hash_set);
            extract_columns(&div.right, plan, hash_set)
        }
        LogicalExpression::Mul(mul) => {
            extract_columns(&mul.left, plan, hash_set);
            extract_columns(&mul.right, plan, hash_set)
        }
        LogicalExpression::Mod(modu) => {
            extract_columns(&modu.left, plan, hash_set);
            extract_columns(&modu.right, plan, hash_set)
        }
        LogicalExpression::Avg(avg) => extract_columns(&avg.expr, plan, hash_set),
        LogicalExpression::Sum(sum) => extract_columns(&sum.expr, plan, hash_set),
        LogicalExpression::Max(max) => extract_columns(&max.expr, plan, hash_set),
        LogicalExpression::Min(min) => extract_columns(&min.expr, plan, hash_set),
        LogicalExpression::Count(count) => extract_columns(&count.expr, plan, hash_set),
    }
}
