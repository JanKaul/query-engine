use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The index `{0}` is out of bounds.")]
    ExceedingBoundsError(usize),
    #[error("Field `{0}` is not contained in logical plan.")]
    NoFieldInLogicalPlan(String),
    #[error("The expressions `{0}` and `{1}` have different sizes.")]
    DifferentSizes(String, String),
    #[error("Physical type `{0}` is not supported.")]
    PhysicalTypeNotSuported(String),
    #[error("Primitive type `{0}` is not supported.")]
    PrimitiveTypeNotSuported(String),
    #[error("Error wile downcasting Array.")]
    DowncastError,
    #[error("IoError: `{0}`.")]
    IoError(std::io::Error),
    #[error("IoError: `{0}`.")]
    ArrowError(arrow2::error::ArrowError),
}
