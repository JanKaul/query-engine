use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The index `{0}` is out of bounds.")]
    ExceedingBoundsError(usize),
    #[error("Field `{0}` is not contained in logical plan.")]
    NoFieldInLogicalPlan(String),
    #[error("The expressions `{0}` and `{1}` have different sizes.")]
    DifferentSizes(String, String),
    #[error("Physical expression `{0}` is not supported.")]
    PhysicalExpressionNotSuported(String),
    #[error("Physical plan `{0}` is not supported.")]
    PhysicalPlanNotSuported(String),
    #[error("Physical type `{0}` is not supported.")]
    PhysicalTypeNotSuported(String),
    #[error("Primitive type `{0}` is not supported.")]
    PrimitiveTypeNotSuported(String),
    #[error("Logical plan `{0}` is missing children.")]
    MissingChildren(String),
    #[error("Physical plan `{0}` is missing its input.")]
    MissingInputPhysicalPlan(String),
    #[error("The hashmap for aggregation is empty.")]
    EmptyHashmapForAggregate,
    #[error("Error wile downcasting Array.")]
    DowncastError,
    #[error("Couldn't convert Scalar value `{0}` to array.")]
    ScalarToArrayError(String),
    #[error("Expr doesn't evaluate to a boolean array, which is needed to filter.")]
    NoBooleanArrayForFilter,
    #[error("IoError: `{0}`.")]
    IoError(std::io::Error),
    #[error("IoError: `{0}`.")]
    ArrowError(arrow2::error::ArrowError),
}
