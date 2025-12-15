use pyo3::exceptions::{PyIOError, PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyString;
use std::io;

pub fn io_err(_py: Python<'_>, err: io::Error) -> PyErr {
    PyIOError::new_err(format!("{}", err))
}

pub fn os_err(_py: Python<'_>, err: io::Error) -> PyErr {
    let errno = err.raw_os_error();
    PyOSError::new_err((errno, format!("{}", err)))
}

pub fn value_err(msg: &str) -> PyErr {
    PyValueError::new_err(msg.to_string())
}

#[allow(dead_code)]
pub fn io_err_ctx(_py: Python<'_>, err: io::Error, context: &str) -> PyErr {
    PyIOError::new_err(format!("{}: {}", context, err))
}

pub fn get_temp_dir() -> String {
    std::env::temp_dir().to_string_lossy().to_string()
}

pub fn path_to_string(path: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = path.cast::<PyString>() {
        Ok(s.str()?.to_string())
    } else if let Ok(has_fspath) = path.hasattr("__fspath__") {
        if has_fspath {
            let fspath_result = path.call_method0("__fspath__")?;
            if let Ok(path_str) = fspath_result.extract::<String>() {
                Ok(path_str)
            } else {
                Err(value_err("path must be a string path or PathLike object"))
            }
        } else {
            Err(value_err("path must be a string path or PathLike object"))
        }
    } else {
        Err(value_err("path must be a string path or PathLike object"))
    }
}

/// Configure std::fs options from mode string
pub fn configure_file_options(
    opts: &mut std::fs::OpenOptions,
    mode: &str,
    for_temp: bool,
) -> PyResult<()> {
    if mode.contains('r') {
        opts.read(true);
        if mode.contains('+') {
            if for_temp {
                opts.write(true).create(true);
            } else {
                opts.write(true);
            }
        }
    }
    if mode.contains('w') {
        opts.write(true).create(true).truncate(true);
        if mode.contains('+') {
            opts.read(true);
        }
    }
    if mode.contains('a') {
        opts.write(true).create(true).append(true);
        if mode.contains('+') {
            opts.read(true);
        }
    }
    if mode.contains('x') {
        opts.write(true).create_new(true);
    }
    Ok(())
}

/// Configure tokio::fs options from mode string
pub fn configure_file_options_async(
    opts: &mut tokio::fs::OpenOptions,
    mode: &str,
    for_temp: bool,
) -> PyResult<()> {
    if mode.contains('r') {
        opts.read(true);
        if mode.contains('+') {
            if for_temp {
                opts.write(true).create(true);
            } else {
                opts.write(true);
            }
        }
    }
    if mode.contains('w') {
        opts.write(true).create(true).truncate(true);
        if mode.contains('+') {
            opts.read(true);
        }
    }
    if mode.contains('a') {
        opts.write(true).create(true).append(true);
        if mode.contains('+') {
            opts.read(true);
        }
    }
    if mode.contains('x') {
        opts.write(true).create_new(true);
    }
    Ok(())
}
