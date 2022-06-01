use std::fs::File;

use crate::error::Error;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileMetaData, FileReader};

pub enum DataSource {
    Parquet(ParquetDataSource),
}

impl DataSource {
    pub fn schema(&self) -> Schema {
        match self {
            DataSource::Parquet(ds) => ds.schema(),
        }
    }
    pub fn scan(self, projection: Option<Vec<String>>) -> FileReader<File> {
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
    pub fn scan(self, projection: Option<Vec<String>>) -> FileReader<File> {
        let projection: Option<Vec<usize>> = projection.map(|projection| {
            self.schema()
                .fields
                .into_iter()
                .enumerate()
                .filter_map(|(i, x)| {
                    if projection.contains(&x.name) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect()
        });
        FileReader::try_new(
            self.file,
            projection.as_ref().map(|x| x.as_slice()),
            None,
            None,
            None,
        )
        .unwrap()
    }
}
