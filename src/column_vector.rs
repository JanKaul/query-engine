pub use crate::error::Error;
use arrow2::datatypes;

pub trait ColumnVector {
    type DataType;
    fn get_type(&self) -> &datatypes::DataType;
    fn get_value(&self, i: usize) -> Result<&Self::DataType, Error>;
    fn size(&self) -> usize;
}

struct LiteralValueVector<T> {
    data_type: datatypes::DataType,
    value: T,
}

impl<T> ColumnVector for LiteralValueVector<T> {
    type DataType = T;
    fn get_type(&self) -> &datatypes::DataType {
        &self.data_type
    }
    fn get_value(&self, i: usize) -> Result<&Self::DataType, Error> {
        if i == 0 {
            Ok(&self.value)
        } else {
            Err(Error::ExceedingBoundsError(i))
        }
    }
    fn size(&self) -> usize {
        1
    }
}
