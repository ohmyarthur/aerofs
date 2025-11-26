use pyo3::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError, PyOSError};
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
pub fn normalize_mode(mode: &str) -> Result<String, PyErr> {
    let mut normalized = mode.to_string();
    if !normalized.contains('b') && !normalized.contains('t') {
        normalized.push('t');
    }
    Ok(normalized)
}

#[allow(dead_code)]
pub fn parse_open_mode(mode: &str) -> Result<(bool, bool, bool, bool, bool), PyErr> {
    let read = mode.contains('r');
    let write = mode.contains('w') || mode.contains('a');
    let append = mode.contains('a');
    let create = mode.contains('w') || mode.contains('a') || mode.contains('x');
    let binary = mode.contains('b');
    
    if mode.contains('x') && mode.contains('w') {
        return Err(value_err("cannot have both write and exclusive create modes"));
    }
    
    Ok((read, write, append, create, binary))
}
