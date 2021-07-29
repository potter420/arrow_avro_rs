use thiserror::Error as ThisError;
use arrow_avro_rs::errors::Error;
use pyo3::{PyErr, exceptions::PyRuntimeError};

#[derive(Debug, ThisError)]
pub enum PyAvroArrowError {
    #[error(transparent)]
    Arrow(#[from] arrow2::error::ArrowError),
    #[error(transparent)]
    Any(#[from] Error),
    #[error("{0}")]
    Other(String),
}

impl std::convert::From<PyAvroArrowError> for PyErr {
    fn from(err: PyAvroArrowError) -> PyErr {
        PyRuntimeError::new_err(format!("{:?}", err))
    }
}