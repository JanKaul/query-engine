use std::sync::Arc;

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::ArrowError,
};

use crate::data_source::DataSource;

pub mod physical_expressions;

pub enum PhysicalPlan {
    Scan(ScanExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
        }
    }
    pub fn children(&self) -> Option<&[PhysicalPlan]> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
        }
    }
}

pub struct ScanExec {
    data_source: DataSource,
    projection: Option<Vec<String>>,
    schema: Schema,
}

impl ScanExec {
    pub fn new(data_source: DataSource, projection: Option<Vec<String>>) -> Self {
        ScanExec {
            schema: Self::derive_schema(&data_source, &projection),
            data_source: data_source,
            projection: projection,
        }
    }

    fn derive_schema(data_source: &DataSource, projection: &Option<Vec<String>>) -> Schema {
        match projection {
            Some(pro) => data_source
                .schema()
                .fields
                .iter()
                .filter(|x| pro.contains(&x.name))
                .map(|y| y.clone())
                .collect::<Vec<Field>>()
                .into(),
            None => data_source.schema(),
        }
    }
}

impl ScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn children(&self) -> Option<&[PhysicalPlan]> {
        None
    }
    fn execute(self) -> impl Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>> {
        self.data_source.scan(self.projection)
    }
}
