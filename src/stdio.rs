use crate::utils::value_err;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};

#[pyclass]
pub struct AsyncStdin {
    is_bytes: bool,
}

#[pymethods]
impl AsyncStdin {
    fn read<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Err::<Py<PyAny>, PyErr>(pyo3::exceptions::PyOSError::new_err(
                "stdin.read() not supported in async context",
            ))
        })
    }

    fn readline<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let is_bytes = self.is_bytes;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stdin = io::stdin();
            let mut reader = io::BufReader::new(stdin);
            let mut buffer = Vec::new();

            reader
                .read_until(b'\n', &mut buffer)
                .await
                .map_err(|e| value_err(&e.to_string()))?;

            Python::attach(|py| {
                if is_bytes {
                    Ok(PyBytes::new(py, &buffer).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&buffer);
                    Ok(PyString::new(py, &s).into_any().unbind())
                }
            })
        })
    }
}

#[pyclass]
pub struct AsyncStdout {
    is_bytes: bool,
}

#[pymethods]
impl AsyncStdout {
    fn write<'a>(&self, py: Python<'a>, data: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let is_bytes = self.is_bytes;
        let data_py = data.unbind();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::attach(|py| {
                let sys = py.import("sys")?;
                let stdout = if is_bytes {
                    sys.getattr("stdout")?.getattr("buffer")?
                } else {
                    sys.getattr("stdout")?
                };
                let result = stdout.call_method1("write", (data_py.bind(py),))?;
                stdout.call_method0("flush")?;
                Ok(result.unbind())
            })
        })
    }

    fn flush<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stdout = io::stdout();
            stdout
                .flush()
                .await
                .map_err(|e| value_err(&e.to_string()))?;
            Ok(Python::attach(|py| py.None()))
        })
    }
}

#[pyclass]
pub struct AsyncStderr {
    is_bytes: bool,
}

#[pymethods]
impl AsyncStderr {
    fn write<'a>(&self, py: Python<'a>, data: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        let is_bytes = self.is_bytes;
        let data_py = data.unbind();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::attach(|py| {
                let sys = py.import("sys")?;
                let stderr = if is_bytes {
                    sys.getattr("stderr")?.getattr("buffer")?
                } else {
                    sys.getattr("stderr")?
                };
                let result = stderr.call_method1("write", (data_py.bind(py),))?;
                stderr.call_method0("flush")?;
                Ok(result.unbind())
            })
        })
    }

    fn flush<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut stderr = io::stderr();
            stderr
                .flush()
                .await
                .map_err(|e| value_err(&e.to_string()))?;
            Ok(Python::attach(|py| py.None()))
        })
    }
}

pub fn register_stdio(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("stdin", Py::new(m.py(), AsyncStdin { is_bytes: false })?)?;
    m.add("stdout", Py::new(m.py(), AsyncStdout { is_bytes: false })?)?;
    m.add("stderr", Py::new(m.py(), AsyncStderr { is_bytes: false })?)?;
    m.add(
        "stdin_bytes",
        Py::new(m.py(), AsyncStdin { is_bytes: true })?,
    )?;
    m.add(
        "stdout_bytes",
        Py::new(m.py(), AsyncStdout { is_bytes: true })?,
    )?;
    m.add(
        "stderr_bytes",
        Py::new(m.py(), AsyncStderr { is_bytes: true })?,
    )?;
    Ok(())
}
