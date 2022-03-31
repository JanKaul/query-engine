use crate::column_vector::ColumnVector;
use crate::error::Error;
use crate::schema::Schema;

pub struct RecordBatch<V: ColumnVector> {
    schema: Schema,
    fields: Vec<V>,
}

impl<T, V: ColumnVector<DataType = T>> RecordBatch<V> {
    fn row_count(&self) -> usize {
        self.fields[0].size()
    }
    fn column_count(&self) -> usize {
        self.fields.len()
    }
    fn field(&self, i: usize) -> Result<&V, Error> {
        if i < self.fields.len() {
            Ok(&self.fields[i])
        } else {
            Err(Error::ExceedingBoundsError(i))
        }
    }
}
