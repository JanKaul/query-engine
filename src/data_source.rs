use crate::column_vector::ColumnVector;
use crate::record_batch::RecordBatch;
use arrow2::datatypes::Schema;

pub(crate) trait DataSource {
    fn schema(&self) -> Schema;
    fn scan<V: ColumnVector>(&self, projection: Vec<String>) -> &[RecordBatch<V>];
}
