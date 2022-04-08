use std::sync::Arc;

use arrow2::{
    array::{Array, BooleanArray},
    chunk::Chunk,
    compute,
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
    Selection(SelectionExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
            PhysicalPlan::Projection(proj) => proj.schema(),
            PhysicalPlan::Selection(sel) => sel.schema(),
        }
    }
    pub fn children(&self) -> Option<&[PhysicalPlan]> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
            PhysicalPlan::Projection(proj) => proj.children(),
            PhysicalPlan::Selection(sel) => sel.children(),
        }
    }
    pub fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        match self {
            PhysicalPlan::Scan(scan) => scan.execute(),
            PhysicalPlan::Projection(proj) => proj.execute(),
            PhysicalPlan::Selection(sel) => sel.execute(),
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

pub struct SelectionExec {
    input: Vec<PhysicalPlan>,
    schema: Schema,
    expr: Box<dyn PhysicalExpression>,
}

impl SelectionExec {
    pub fn new(
        input: Vec<PhysicalPlan>,
        expr: Box<dyn PhysicalExpression>,
        schema: Schema,
    ) -> Self {
        SelectionExec {
            schema: schema,
            input: input,
            expr: expr,
        }
    }
}

pub struct SelectionIterator<I: Iterator<Item = Batch>> {
    inputIter: I,
    expr: Box<dyn PhysicalExpression>,
}

impl<I: Iterator<Item = Batch>> Iterator for SelectionIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.inputIter.next() {
            Some(res) => Some(res.and_then(|chunk| {
                let bitvector = self.expr.evaluate(&chunk).and_then(|col| match col {
                    ColumnarValue::Array(array) => Ok(array),
                    ColumnarValue::Scalar(scalar) => Ok(scalar_to_array(scalar, chunk.len())?),
                })?;
                Ok(Chunk::new(
                    compute::filter::filter_chunk(
                        &chunk,
                        bitvector
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or(Error::NoBooleanArrayForFilter)?,
                    )
                    .map_err(|err| Error::ArrowError(err))?
                    .into_arrays()
                    .into_iter()
                    .map(|array| Arc::from(array) as Arc<dyn Array>)
                    .collect::<Vec<Arc<dyn Array>>>(),
                ))
            })),
            None => None,
        }
    }
}

impl SelectionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn children(&self) -> Option<&[PhysicalPlan]> {
        Some(&self.input)
    }
    fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        let mut vec = self.input;
        let input = vec.pop().unwrap();
        Box::new(SelectionIterator {
            inputIter: input.execute(),
            expr: self.expr,
        })
    }
}
