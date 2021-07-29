use std::sync::Arc;
use arrow2::ffi;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use arrow_avro_rs::io::{read_into_struct};
use pyo3::ffi::Py_uintptr_t;
use crate::errors::PyAvroArrowError;
use arrow2::array::Array;

mod errors;

type ArrayRef = Arc<dyn Array>;

fn to_py(array: ArrayRef, py: Python) -> PyResult<PyObject> {
    let array_ptr = ffi::export_to_c(array).map_err(PyAvroArrowError::from)?;

    let (array_ptr, schema_ptr) = array_ptr.references();

    let pa = py.import("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

#[pyfunction]
fn read_avro_struct(py: Python, file_path: &str) -> PyResult<PyObject> {
    let f = std::fs::File::open(file_path)?;
    // import
    let array = read_into_struct(f).map_err(PyAvroArrowError::from)?;
    let array = to_py(Arc::new(array), py)?;
    Ok(array)
}


#[pymodule]
fn arrow_avro_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(read_avro_struct)).unwrap();
    Ok(())
}