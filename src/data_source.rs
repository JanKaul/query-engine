use std::fs::File;

use crate::column_vector::ColumnVector;
use crate::error::Error;
use crate::record_batch::RecordBatch;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileMetaData};

pub trait DataSource {
    fn schema(&self) -> Schema;
    fn scan<T, V: ColumnVector<DataType = T>>(
        &self,
        projection: Vec<String>,
    ) -> &[RecordBatch<T, V>];
}

pub struct ParquetDataSource {
    file: File,
    metadata: FileMetaData,
}

impl ParquetDataSource {
    pub fn new(path: &str) -> Result<Self, Error> {
        match File::open(path) {
            Ok(mut file) => {
                let metadata = read_metadata(&mut file).map_err(|err| Error::ArrowError(err))?;
                Ok(ParquetDataSource {
                    file: file,
                    metadata: metadata,
                })
            }
            Err(err) => Err(Error::IoError(err)),
        }
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Schema {
        infer_schema(&self.metadata).unwrap()
    }
    fn scan<T, V: ColumnVector<DataType = T>>(
        &self,
        projection: Vec<String>,
    ) -> &[RecordBatch<T, V>] {
        todo!()
    }
}
