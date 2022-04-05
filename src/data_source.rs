use std::fs::File;

use crate::error::Error;
use crate::record_batch::RecordBatch;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileMetaData};

pub enum DataSource {
    Parquet(ParquetDataSource),
}

impl DataSource {
    pub fn schema(&self) -> Schema {
        match self {
            DataSource::Parquet(ds) => ds.schema(),
        }
    }
    pub fn scan(&self, projection: Vec<String>) -> &[RecordBatch] {
        match self {
            DataSource::Parquet(ds) => ds.scan(projection),
        }
    }
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

impl ParquetDataSource {
    fn schema(&self) -> Schema {
        infer_schema(&self.metadata).unwrap()
    }
    fn scan(&self, projection: Vec<String>) -> &[RecordBatch] {
        todo!()
    }
}
