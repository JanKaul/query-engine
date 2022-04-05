use crate::error::Error;
use arrow2::array::Array;
use arrow2::datatypes::Schema;

pub struct RecordBatch {
    schema: Schema,
    fields: Vec<Box<dyn Array>>,
}

impl RecordBatch {
    fn row_count(&self) -> usize {
        self.fields[0].len()
    }
    fn column_count(&self) -> usize {
        self.fields.len()
    }
    pub fn field(&self, i: usize) -> Result<&dyn Array, Error> {
        if i < self.fields.len() {
            Ok(&*self.fields[i])
        } else {
            Err(Error::ExceedingBoundsError(i))
        }
    }
}
