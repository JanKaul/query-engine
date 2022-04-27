use std::collections::HashSet;
use std::sync::Arc;
use std::{borrow::Borrow, collections::HashMap};

use arrow2::array::{
    MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray,
    Utf8Array,
};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::compute::arithmetics::ArrayAdd;
use arrow2::datatypes::{DataType, PhysicalType, PrimitiveType};
use arrow2::scalar::PrimitiveScalar;
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

use self::physical_expressions::{Accumulator, PhysicalAggregateExpression, PhysicalExpression};

pub mod physical_expressions;

type Batch = Result<Chunk<Arc<dyn Array>>, Error>;

pub enum PhysicalPlan {
    Scan(ScanExec),
    Projection(ProjectionExec),
    Selection(SelectionExec),
    Aggregate(AggregateExec),
}

impl PhysicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            PhysicalPlan::Scan(scan) => scan.schema(),
            PhysicalPlan::Projection(proj) => proj.schema(),
            PhysicalPlan::Selection(sel) => sel.schema(),
            PhysicalPlan::Aggregate(agg) => agg.schema(),
        }
    }
    pub fn children(&self) -> Option<&[PhysicalPlan]> {
        match self {
            PhysicalPlan::Scan(scan) => scan.children(),
            PhysicalPlan::Projection(proj) => proj.children(),
            PhysicalPlan::Selection(sel) => sel.children(),
            PhysicalPlan::Aggregate(agg) => agg.children(),
        }
    }
    pub fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        match self {
            PhysicalPlan::Scan(scan) => scan.execute(),
            PhysicalPlan::Projection(proj) => proj.execute(),
            PhysicalPlan::Selection(sel) => sel.execute(),
            PhysicalPlan::Aggregate(agg) => agg.execute(),
        }
    }
}

pub struct ScanExec {
    pub(crate) data_source: DataSource,
    pub(crate) projection: Option<Vec<String>>,
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
    input_iter: I,
}

impl<I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>>> Iterator for ScanIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.input_iter.next() {
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
            input_iter: self.data_source.scan(self.projection),
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
    input_iter: I,
    exprs: Vec<Box<dyn PhysicalExpression>>,
}

impl<I: Iterator<Item = Batch>> Iterator for ProjectionIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.input_iter.next() {
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
            input_iter: input.execute(),
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
    input_iter: I,
    expr: Box<dyn PhysicalExpression>,
}

impl<I: Iterator<Item = Batch>> Iterator for SelectionIterator<I> {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        match self.input_iter.next() {
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
            input_iter: input.execute(),
            expr: self.expr,
        })
    }
}

pub struct AggregateExec {
    input: Vec<PhysicalPlan>,
    schema: Schema,
    group_exprs: Vec<Box<dyn PhysicalExpression>>,
    agg_exprs: Vec<Box<dyn PhysicalAggregateExpression>>,
}

impl AggregateExec {
    pub fn new(
        input: Vec<PhysicalPlan>,
        group_exprs: Vec<Box<dyn PhysicalExpression>>,
        agg_exprs: Vec<Box<dyn PhysicalAggregateExpression>>,
        schema: Schema,
    ) -> Self {
        AggregateExec {
            schema: schema,
            input: input,
            group_exprs: group_exprs,
            agg_exprs: agg_exprs,
        }
    }
}

pub struct AggregateIterator {
    output: Option<Batch>,
}

impl Iterator for AggregateIterator {
    type Item = Batch;
    fn next(&mut self) -> Option<Self::Item> {
        self.output.take()
    }
}

impl AggregateExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }
    fn children(&self) -> Option<&[PhysicalPlan]> {
        Some(&self.input)
    }
    fn execute(self) -> Box<dyn Iterator<Item = Batch>> {
        let mut vec = self.input;
        let input = vec.pop().unwrap();
        let mut hashmap = HashMap::new();
        input.execute().for_each(|res| match res {
            Ok(batch) => {
                let length = batch.len();
                let mut hashset = HashSet::new();
                let group_keys = Chunk::new(
                    self.group_exprs
                        .iter()
                        .map(|expr| expr.evaluate(&batch).map(|x| x.to_array(length)))
                        .collect::<Result<Vec<Arc<dyn Array>>, Error>>()
                        .unwrap_or(Vec::new()),
                );
                let group_hashes = group_keys.iter().fold(
                    compute::hash::hash_primitive::<u64>(&PrimitiveArray::new(
                        DataType::UInt64,
                        Buffer::new_zeroed(length),
                        None,
                    )),
                    |acc: PrimitiveArray<u64>, x| {
                        let new = compute::hash::hash(x.borrow()).unwrap();
                        acc.add(&new)
                    },
                );
                let agg_input = self
                    .agg_exprs
                    .iter()
                    .map(|expr| expr.evaluate(&batch))
                    .collect::<Result<Vec<ColumnarValue>, Error>>()
                    .unwrap_or(Vec::new());
                group_hashes
                    .iter()
                    .enumerate()
                    .for_each(|(i, key)| match key {
                        Some(key) => {
                            if !hashset.contains(key) {
                                hashset.insert(key);
                                let validity = Bitmap::from_trusted_len_iter(
                                    compute::comparison::eq_scalar(
                                        &group_hashes,
                                        &PrimitiveScalar::new(DataType::UInt64, Some(key.clone())),
                                    )
                                    .values_iter(),
                                );
                                if !hashmap.contains_key(key) {
                                    let accumulators = self
                                        .agg_exprs
                                        .iter()
                                        .enumerate()
                                        .map(|(i, x)| x.create_accumulator(i))
                                        .collect::<Vec<_>>();
                                    let group_keys = group_keys
                                        .iter()
                                        .map(|x| x.slice(i, 1))
                                        .map(|y| Arc::from(y))
                                        .collect::<Vec<_>>();
                                    hashmap.insert(*key, (accumulators, group_keys));
                                }
                                hashmap.get_mut(key).map(|(accs, _)| {
                                    accs.iter_mut().for_each(|acc| {
                                        acc.accumulate(&agg_input, Some(&validity)).unwrap();
                                    })
                                });
                                ()
                            }
                        }
                        None => (),
                    })
            }
            Err(_) => (),
        });
        let rows = hashmap.len();
        let mut iter = hashmap.into_values();
        let (accs, mut groups) = iter.next().unwrap();
        accs.into_iter()
            .for_each(|x| groups.push(x.final_value().unwrap().to_array(1)));
        let mut columns = groups
            .into_iter()
            .map(|col| match col.data_type().to_physical_type() {
                PhysicalType::Primitive(PrimitiveType::Int32) => {
                    let mut mutable_array =
                        MutablePrimitiveArray::with_capacity_from(rows, DataType::Int32);
                    mutable_array.extend_from_slice(
                        col.as_any()
                            .downcast_ref::<PrimitiveArray<i32>>()
                            .unwrap()
                            .values(),
                    );
                    Ok(Box::new(mutable_array) as Box<dyn MutableArray>)
                }
                PhysicalType::Primitive(PrimitiveType::Float64) => {
                    let mut mutable_array =
                        MutablePrimitiveArray::with_capacity_from(rows, DataType::Float64);
                    mutable_array.extend_from_slice(
                        col.as_any()
                            .downcast_ref::<PrimitiveArray<f64>>()
                            .unwrap()
                            .values(),
                    );
                    Ok(Box::new(mutable_array) as Box<dyn MutableArray>)
                }
                PhysicalType::Utf8 => {
                    let mut mutable_array = MutableUtf8Array::<i32>::with_capacity(rows);
                    mutable_array.extend_trusted_len(
                        col.as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap()
                            .iter(),
                    );
                    Ok(Box::new(mutable_array) as Box<dyn MutableArray>)
                }
                PhysicalType::Boolean => {
                    let mut mutable_array = MutableBooleanArray::with_capacity(rows);
                    mutable_array.extend_trusted_len(
                        col.as_any().downcast_ref::<BooleanArray>().unwrap().iter(),
                    );
                    Ok(Box::new(mutable_array) as Box<dyn MutableArray>)
                }
                t => Err(Error::PhysicalTypeNotSuported(format!("{:?}", t))),
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        iter.for_each(|(accs, mut groups)| {
            accs.into_iter()
                .for_each(|x| groups.push(x.final_value().unwrap().to_array(1)));
            groups
                .into_iter()
                .zip(columns.iter_mut())
                .for_each(|(new, col)| match col.data_type().to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::Int32) => {
                        col.as_mut_any()
                            .downcast_mut::<MutablePrimitiveArray<i32>>()
                            .unwrap()
                            .extend_from_slice(
                                new.as_any()
                                    .downcast_ref::<PrimitiveArray<i32>>()
                                    .unwrap()
                                    .values(),
                            );
                    }
                    PhysicalType::Primitive(PrimitiveType::Float64) => {
                        col.as_mut_any()
                            .downcast_mut::<MutablePrimitiveArray<f64>>()
                            .unwrap()
                            .extend_from_slice(
                                new.as_any()
                                    .downcast_ref::<PrimitiveArray<f64>>()
                                    .unwrap()
                                    .values(),
                            );
                    }
                    PhysicalType::Utf8 => {
                        col.as_mut_any()
                            .downcast_mut::<MutableUtf8Array<i32>>()
                            .unwrap()
                            .extend_trusted_len(
                                new.as_any()
                                    .downcast_ref::<Utf8Array<i32>>()
                                    .unwrap()
                                    .iter(),
                            );
                    }
                    PhysicalType::Boolean => {
                        col.as_mut_any()
                            .downcast_mut::<MutableBooleanArray>()
                            .unwrap()
                            .extend_trusted_len(
                                new.as_any().downcast_ref::<BooleanArray>().unwrap().iter(),
                            );
                    }
                    _ => (),
                });
        });
        let columns = Chunk::new(
            columns
                .into_iter()
                .map(|mut col| col.as_arc())
                .collect::<Vec<Arc<_>>>(),
        );
        Box::new(AggregateIterator {
            output: Some(Ok(columns)),
        })
    }
}
