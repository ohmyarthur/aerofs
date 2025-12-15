use pyo3::prelude::*;

mod file_ops;
mod os_ops;
mod stdio;
mod tempfile_ops;
mod utils;

#[pymodule]
fn _aerofs(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(file_ops::open, m)?)?;

    let os_module = PyModule::new(py, "os")?;
    os_ops::register_os_module(py, &os_module)?;
    m.add_submodule(&os_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("aerofs._aerofs.os", &os_module)?;

    let tempfile_module = PyModule::new(py, "tempfile")?;
    tempfile_ops::register_tempfile_module(py, &tempfile_module)?;
    m.add_submodule(&tempfile_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("aerofs._aerofs.tempfile", &tempfile_module)?;

    stdio::register_stdio(m)?;

    Ok(())
}
