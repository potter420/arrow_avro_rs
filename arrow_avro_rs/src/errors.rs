use std::borrow::Cow;
use thiserror::Error as ThisError;

type ErrString = Cow<'static, str>;
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    ArrowError(#[from] arrow2::error::ArrowError),
    #[error(transparent)]
    AvroError(#[from] avro_rs::Error),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("Reader Error {0}")]
    ReaderError(ErrString),
    #[error("Writer Error {0}")]
    WriterError(ErrString),
}
