use arrow2::datatypes::Schema;

use crate::record_batch::RecordBatch;

pub mod physical_expressions;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;
    fn execute(self) -> Vec<RecordBatch>;
    fn children(&self) -> Option<&[Box<dyn PhysicalPlan>]>;
}
