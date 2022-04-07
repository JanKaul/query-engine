use std::sync::Arc;

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::ArrowError,
};

use crate::{
    columnar_value::{scalar_to_array, ColumnarValue},
    data_source::DataSource,
    error::Error,
};

use self::physical_expressions::PhysicalExpression;

pub mod physical_expressions;

type Batch = Result<Chunk<Arc<dyn Array>>, Error>;

pub enum PhysicalPlan {
    Scan(ScanExec),
    Projection(ProjectionExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
            PhysicalPlan::Projection(proj) => proj.schema(),
        }
    }
    pub fn children(&self) -> Option<&[PhysicalPlan]> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
            PhysicalPlan::Projection(proj) => proj.children(),
        }
    }
    pub fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        match self {
            PhysicalPlan::Scan(scan) => scan.execute(),
            PhysicalPlan::Projection(proj) => proj.execute(),
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

pub struct ScanIterator<I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>>> {
    inputIter: I,
}

impl<I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>>> Iterator for ScanIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inputIter.next() {
            Some(chunk) => Some(chunk.map_err(|err| Error::ArrowError(err))),
            None => None,
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
    fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        Box::new(ScanIterator {
            inputIter: self.data_source.scan(self.projection),
        })
    }
}

pub struct ProjectionExec {
    input: Vec<PhysicalPlan>,
    schema: Schema,
    exprs: Vec<Box<dyn PhysicalExpression>>,
}

impl ProjectionExec {
    pub fn new(
        input: Vec<PhysicalPlan>,
        exprs: Vec<Box<dyn PhysicalExpression>>,
        schema: Schema,
    ) -> Self {
        ProjectionExec {
            schema: schema,
            input: input,
            exprs: exprs,
        }
    }
}

pub struct ProjectionIterator<I: Iterator<Item = Batch>> {
    inputIter: I,
    exprs: Vec<Box<dyn PhysicalExpression>>,
}

impl<I: Iterator<Item = Batch>> Iterator for ProjectionIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inputIter.next() {
            Some(res) => Some(res.and_then(|chunk| {
                self.exprs
                    .iter()
                    .map(|expr| {
                        expr.evaluate(&chunk).and_then(|col| match col {
                            ColumnarValue::Array(array) => Ok(array),
                            ColumnarValue::Scalar(scalar) => {
                                Ok(scalar_to_array(scalar, chunk.len())?)
                            }
                        })
                    })
                    .collect::<Result<Vec<Arc<dyn Array>>, Error>>()
                    .map(|v| Chunk::new(v))
            })),
            None => None,
        }
    }
}

impl ProjectionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn children(&self) -> Option<&[PhysicalPlan]> {
        Some(&self.input)
    }
    fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        let mut vec = self.input;
        let input = vec.pop().unwrap();
        Box::new(ProjectionIterator {
            inputIter: input.execute(),
            exprs: self.exprs,
        })
    }
}
