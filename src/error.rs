use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The index `{0}` is out of bounds")]
    ExceedingBoundsError(usize),
}
