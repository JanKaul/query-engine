use std::fs::File;
use std::sync::Arc;

use crate::error::Error;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::ArrowError;
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
    pub fn scan<I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>>>(
        &self,
        projection: Vec<String>,
    ) -> I {
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
    pub fn scan<I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>>>(
        &self,
        projection: Vec<String>,
    ) -> I {
        todo!()
    }
}
