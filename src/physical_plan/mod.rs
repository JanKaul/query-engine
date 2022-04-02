use arrow2::datatypes::Schema;

use crate::{column_vector::ColumnVector, record_batch::RecordBatch};

pub mod expressions;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;

    fn children(&self) -> Option<&[Box<dyn PhysicalPlan>]>;
}

pub trait ExecutePhysicalPlan: PhysicalPlan {
    fn execute<T, V: ColumnVector<DataType = T>>(self) -> Vec<RecordBatch<T, V>>;
}
