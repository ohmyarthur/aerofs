use crate::utils::{io_err, value_err};
use pyo3::conversion::IntoPyObject;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyBytesMethods, PyList, PyString};
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

const BUFFER_SIZE: usize = 131072;
const BUFFER_POOL_MAX: usize = 8;

thread_local! {
    static BUFFER_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

fn acquire_buffer(capacity: usize) -> Vec<u8> {
    BUFFER_POOL.with(|pool| {
        pool.borrow_mut()
            .pop()
            .map(|mut buf| {
                buf.clear();
                if buf.capacity() < capacity {
                    buf.reserve(capacity - buf.capacity());
                }
                buf
            })
            .unwrap_or_else(|| Vec::with_capacity(capacity))
    })
}

fn release_buffer(buf: Vec<u8>) {
    BUFFER_POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if p.len() < BUFFER_POOL_MAX {
            p.push(buf);
        }
    });
}

/// UTF-8 string conversion with fast-path for valid UTF-8
fn bytes_to_pystring(py: Python<'_>, bytes: &[u8]) -> Py<PyAny> {
    match std::str::from_utf8(bytes) {
        Ok(s) => PyString::new(py, s).into_any().unbind(),
        Err(_) => {
            let s = String::from_utf8_lossy(bytes);
            PyString::new(py, &s).into_any().unbind()
        }
    }
}

enum FileState {
    BufferedRead(BufReader<File>),
    Raw(File),
}

impl FileState {
    /// Write buffer to file
    fn write_buffer(&mut self, buffer: &[u8]) -> std::io::Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        match self {
            FileState::BufferedRead(r) => r.get_mut().write_all(buffer),
            FileState::Raw(f) => f.write_all(buffer),
        }
    }

    /// Flush to disk
    fn flush_file(&mut self) -> std::io::Result<()> {
        match self {
            FileState::BufferedRead(r) => r.get_mut().flush(),
            FileState::Raw(f) => f.flush(),
        }
    }

    /// Read into buffer
    fn read_bytes(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            FileState::BufferedRead(r) => r.read(buf),
            FileState::Raw(f) => f.read(buf),
        }
    }

    /// Read all remaining bytes
    fn read_all(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            FileState::BufferedRead(r) => r.read_to_end(buf),
            FileState::Raw(f) => f.read_to_end(buf),
        }
    }

    /// Seek to position
    fn seek_to(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self {
            FileState::BufferedRead(r) => r.seek(pos),
            FileState::Raw(f) => f.seek(pos),
        }
    }

    /// Get current position
    fn position(&mut self) -> std::io::Result<u64> {
        match self {
            FileState::BufferedRead(r) => r.stream_position(),
            FileState::Raw(f) => f.stream_position(),
        }
    }

    /// Read until newline
    fn read_line(&mut self, line: &mut Vec<u8>) -> std::io::Result<usize> {
        match self {
            FileState::BufferedRead(r) => r.read_until(b'\n', line),
            FileState::Raw(f) => {
                let mut byte = [0u8; 1];
                let mut count = 0;
                loop {
                    let n = f.read(&mut byte)?;
                    if n == 0 {
                        break;
                    }
                    line.push(byte[0]);
                    count += 1;
                    if byte[0] == b'\n' {
                        break;
                    }
                }
                Ok(count)
            }
        }
    }

    /// Get raw fd
    #[cfg(unix)]
    fn raw_fd(&self) -> i32 {
        match self {
            FileState::BufferedRead(r) => r.get_ref().as_raw_fd(),
            FileState::Raw(f) => f.as_raw_fd(),
        }
    }
}

#[pyclass]
pub struct AsyncFile {
    file: Option<Arc<Mutex<FileState>>>,
    path: PathBuf,
    mode: String,
    #[allow(dead_code)]
    encoding: Option<String>,
    #[allow(dead_code)]
    errors: Option<String>,
    #[allow(dead_code)]
    newline: Option<String>,
    #[allow(dead_code)]
    buffering: i32,
    is_binary: bool,
    closed: bool,
    detached: bool,
    write_buffer: Vec<u8>,
}

#[pymethods]
impl AsyncFile {
    fn init_file<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let path = slf.path.clone();
        let mode = slf.mode.clone();
        let py_obj: Py<AsyncFile> = slf.into();

        let path_clone = path.clone();
        let mode_clone = mode.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_state = tokio::task::spawn_blocking(move || {
                let mut options = std::fs::OpenOptions::new();
                crate::utils::configure_file_options(&mut options, &mode_clone, false)?;

                let file = options.open(&path_clone)?;

                if mode_clone == "r" || mode_clone == "rb" {
                    Ok(FileState::BufferedRead(BufReader::with_capacity(
                        BUFFER_SIZE,
                        file,
                    )))
                } else {
                    Ok(FileState::Raw(file))
                }
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e: std::io::Error| {
                Python::attach(|_py| {
                    let errno = e.raw_os_error();
                    let msg = match e.kind() {
                        std::io::ErrorKind::NotFound => {
                            format!("No such file or directory: '{}'", path.display())
                        }
                        std::io::ErrorKind::PermissionDenied => {
                            format!("Permission denied: '{}'", path.display())
                        }
                        _ => format!("{}: '{}'", e, path.display()),
                    };
                    pyo3::exceptions::PyOSError::new_err((errno, msg))
                })
            })?;

            Python::attach(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.file = Some(Arc::new(Mutex::new(file_state)));
                Ok(py_obj.clone_ref(py))
            })
        })
    }

    fn __aenter__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if slf.file.is_some() {
            let py_obj: Py<AsyncFile> = slf.into();
            return pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(py_obj) });
        }
        Self::init_file(slf, py)
    }

    fn __await__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if slf.file.is_some() {
            let py_obj: Py<AsyncFile> = slf.into();
            let future = pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(py_obj) })?;
            return future.call_method0("__await__");
        }

        let future = Self::init_file(slf, py)?;
        future.call_method0("__await__")
    }

    fn __aexit__<'a>(
        mut slf: PyRefMut<'a, Self>,
        py: Python<'a>,
        _exc_type: Bound<'a, PyAny>,
        _exc_val: Bound<'a, PyAny>,
        _exc_tb: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        if slf.detached {
            return Err(value_err("I/O operation on closed file"));
        }

        let file = slf.file.take();
        let buffer = std::mem::take(&mut slf.write_buffer);
        slf.closed = true;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(file_arc) = file {
                tokio::task::spawn_blocking(move || {
                    let mut state = file_arc.blocking_lock();
                    state
                        .write_buffer(&buffer)
                        .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    state.flush_file().ok();
                    Ok::<(), PyErr>(())
                })
                .await
                .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))??;
            }
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn close<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let file_handle = self.file.take();
        let buffer = std::mem::take(&mut self.write_buffer);
        self.closed = true;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(file_arc) = file_handle {
                tokio::task::spawn_blocking(move || {
                    let mut state = file_arc.blocking_lock();
                    state
                        .write_buffer(&buffer)
                        .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    state
                        .flush_file()
                        .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    Ok::<(), PyErr>(())
                })
                .await
                .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))??;
            }
            Ok(Python::attach(|py| py.None()))
        })
    }

    #[pyo3(signature = ())]
    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let buffer = std::mem::take(&mut self.write_buffer);
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                state.write_buffer(&buffer)?;
                state.flush_file()?;
                Ok::<(), std::io::Error>(())
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Ok(Python::attach(|py| py.None()))
        })
    }

    #[pyo3(signature = (size=None))]
    fn read<'a>(&mut self, py: Python<'a>, size: Option<i64>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        if self.mode == "w" || self.mode == "a" || self.mode == "wb" || self.mode == "ab" {
            return Err(value_err("not readable"));
        }

        let is_binary = self.is_binary;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        let size_opt = size;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(n) = size_opt {
                if n >= 0 {
                    let size = n as usize;

                    let (bytes_obj, buffer_ptr) = Python::attach(|py| unsafe {
                        let ptr = ffi::PyBytes_FromStringAndSize(std::ptr::null(), size as isize);
                        if ptr.is_null() {
                            return Err(PyErr::fetch(py));
                        }
                        let buffer = ffi::PyBytes_AsString(ptr);
                        let obj = Py::from_owned_ptr(py, ptr);
                        Ok((obj, buffer))
                    })?;

                    let buffer_ptr_int = buffer_ptr as usize;
                    let file_arc_clone = file_arc.clone();

                    let bytes_read = tokio::task::spawn_blocking(move || {
                        let mut state = file_arc_clone.blocking_lock();
                        let buffer_slice = unsafe {
                            std::slice::from_raw_parts_mut(buffer_ptr_int as *mut u8, size)
                        };
                        state.read_bytes(buffer_slice)
                    })
                    .await
                    .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
                    .map_err(|e| Python::attach(|py| io_err(py, e)))?;

                    if bytes_read < size {
                        return Python::attach(|py| {
                            let _bytes_ref = bytes_obj.bind(py);
                            let full_slice = unsafe {
                                std::slice::from_raw_parts(buffer_ptr_int as *const u8, size)
                            };
                            let data_slice = &full_slice[0..bytes_read];

                            if is_binary {
                                Ok(PyBytes::new(py, data_slice).into_any().unbind())
                            } else {
                                let s = std::str::from_utf8(data_slice).map_err(|e| {
                                    pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string())
                                })?;
                                Ok(PyString::new(py, s).into_any().unbind())
                            }
                        });
                    } else {
                        return Python::attach(|py| {
                            if is_binary {
                                Ok(bytes_obj.into_any())
                            } else {
                                let bytes_ref = bytes_obj.bind(py);
                                let s = std::str::from_utf8(PyBytesMethods::as_bytes(bytes_ref))
                                    .map_err(|e| {
                                        pyo3::exceptions::PyUnicodeDecodeError::new_err(
                                            e.to_string(),
                                        )
                                    })?;
                                Ok(PyString::new(py, s).into_any().unbind())
                            }
                        });
                    }
                }
            }

            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                let mut buffer = acquire_buffer(BUFFER_SIZE);
                state.read_all(&mut buffer)?;
                Ok::<Vec<u8>, std::io::Error>(buffer)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            let result = Python::attach(|py| {
                if is_binary {
                    Ok(PyBytes::new(py, &buffer).into_any().unbind())
                } else {
                    Ok(bytes_to_pystring(py, &buffer))
                }
            });
            release_buffer(buffer);
            result
        })
    }

    #[pyo3(signature = (size=None))]
    fn read1<'a>(&mut self, py: Python<'a>, size: Option<usize>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let size = size.unwrap_or(8192);
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                let mut buffer = vec![0u8; size];
                let bytes_read = state.read_bytes(&mut buffer)?;
                buffer.truncate(bytes_read);
                Ok::<Vec<u8>, std::io::Error>(buffer)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                if is_binary {
                    Ok(PyBytes::new(py, &buffer).into_any().unbind())
                } else {
                    Ok(bytes_to_pystring(py, &buffer))
                }
            })
        })
    }

    fn readall<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        self.read(py, Some(-1))
    }

    #[pyo3(signature = (size=None))]
    fn readline<'a>(&mut self, py: Python<'a>, size: Option<i64>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let line = tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                let mut line = Vec::new();

                let _bytes_read = if let Some(n) = size {
                    if n <= 0 {
                        state.read_line(&mut line)?
                    } else {
                        for _ in 0..n {
                            let mut byte = [0u8; 1];
                            let n_read = state.read_bytes(&mut byte)?;
                            if n_read == 0 {
                                break;
                            }
                            line.push(byte[0]);
                            if byte[0] == b'\n' {
                                break;
                            }
                        }
                        line.len()
                    }
                } else {
                    state.read_line(&mut line)?
                };
                Ok::<Vec<u8>, std::io::Error>(line)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                if is_binary {
                    Ok(PyBytes::new(py, &line).into_any().unbind())
                } else {
                    Ok(bytes_to_pystring(py, &line))
                }
            })
        })
    }

    #[pyo3(signature = (hint=None))]
    fn readlines<'a>(&mut self, py: Python<'a>, hint: Option<i64>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();

            let lines = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut lines: Vec<Vec<u8>> = Vec::new();
                let mut total_size = 0usize;
                let limit = hint.unwrap_or(-1);

                loop {
                    let mut line_buffer = Vec::new();
                    let bytes_read = match &mut *state {
                        FileState::BufferedRead(r) => r.read_until(b'\n', &mut line_buffer)?,
                        FileState::Raw(f) => {
                            let mut byte = [0u8; 1];
                            let mut count = 0;
                            loop {
                                let n = f.read(&mut byte)?;
                                if n == 0 {
                                    break;
                                }
                                line_buffer.push(byte[0]);
                                count += 1;
                                if byte[0] == b'\n' {
                                    break;
                                }
                            }
                            count
                        }
                    };

                    if bytes_read == 0 {
                        break;
                    }

                    total_size += bytes_read;
                    lines.push(line_buffer);

                    if limit > 0 && total_size >= limit as usize {
                        break;
                    }
                }
                Ok::<Vec<Vec<u8>>, std::io::Error>(lines)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                let items: Vec<Py<PyAny>> = lines
                    .iter()
                    .map(|line| {
                        if is_binary {
                            PyBytes::new(py, line).into_any().unbind()
                        } else {
                            bytes_to_pystring(py, line)
                        }
                    })
                    .collect();
                Ok(PyList::new(py, &items)?.unbind())
            })
        })
    }

    fn readinto<'a>(
        &mut self,
        py: Python<'a>,
        buffer: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let buffer_len = buffer.len()?;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        let buffer_py = buffer.unbind();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();

            let (bytes_read, temp_buffer) = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut temp_buffer = vec![0u8; buffer_len];

                let n = match &mut *state {
                    FileState::BufferedRead(r) => r.read(&mut temp_buffer),
                    FileState::Raw(f) => f.read(&mut temp_buffer),
                }?;

                Ok::<(usize, Vec<u8>), std::io::Error>((n, temp_buffer))
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                let buffer = buffer_py.bind(py);
                if let Ok(bytearray) = buffer.cast::<PyByteArray>() {
                    let current_len = bytearray.len();
                    let write_len = std::cmp::min(current_len, bytes_read);

                    unsafe {
                        let ptr = bytearray.as_bytes_mut().as_mut_ptr();
                        std::ptr::copy_nonoverlapping(temp_buffer.as_ptr(), ptr, write_len);
                    }
                    Ok(write_len.into_pyobject(py)?.unbind())
                } else {
                    Err(value_err(
                        "Only bytearray is supported for readinto currently",
                    ))
                }
            })
        })
    }

    fn write<'a>(&mut self, py: Python<'a>, data: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let bytes_len = data.len()?;

        if self.buffering == 0 {
            let bytes = if is_binary {
                data.cast::<PyBytes>()
                    .map_err(|_| value_err("expected bytes"))?
                    .as_bytes()
                    .to_vec()
            } else {
                let s = data
                    .cast::<PyString>()
                    .map_err(|_| value_err("expected str"))?
                    .str()?
                    .to_string();
                s.into_bytes()
            };

            let file_arc = self
                .file
                .as_ref()
                .ok_or_else(|| value_err("File not open"))?
                .clone();

            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                let bytes_vec = bytes;
                let bytes_written = tokio::task::spawn_blocking(move || {
                    let mut state = file_arc.blocking_lock();
                    match &mut *state {
                        FileState::BufferedRead(_) => Err(pyo3::exceptions::PyIOError::new_err(
                            "File not open for writing",
                        )),
                        FileState::Raw(f) => f
                            .write(&bytes_vec)
                            .map_err(|e| Python::attach(|py| io_err(py, e))),
                    }
                })
                .await
                .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))??;

                Ok(Python::attach(|py| {
                    bytes_written.into_pyobject(py).unwrap().to_owned().unbind()
                }))
            })
        } else {
            if is_binary {
                let bytes_obj = data
                    .cast::<PyBytes>()
                    .map_err(|_| value_err("expected bytes"))?;
                self.write_buffer.extend_from_slice(bytes_obj.as_bytes());
            } else {
                let s_obj = data
                    .cast::<PyString>()
                    .map_err(|_| value_err("expected str"))?;
                let s = s_obj.to_cow()?;
                self.write_buffer.extend_from_slice(s.as_bytes());
            }

            if self.write_buffer.len() >= BUFFER_SIZE {
                let buffer =
                    std::mem::replace(&mut self.write_buffer, Vec::with_capacity(BUFFER_SIZE));
                let file_arc = self
                    .file
                    .as_ref()
                    .ok_or_else(|| value_err("File not open"))?
                    .clone();

                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let mut state = file_arc.lock().await;
                    match &mut *state {
                        FileState::BufferedRead(_) => {
                            return Err(pyo3::exceptions::PyIOError::new_err(
                                "File not open for writing",
                            ));
                        }
                        FileState::Raw(f) => {
                            f.write_all(&buffer)
                                .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                        }
                    }
                    Ok(Python::attach(|py| {
                        bytes_len.into_pyobject(py).unwrap().to_owned().unbind()
                    }))
                })
            } else {
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    Ok(Python::attach(|py| {
                        bytes_len.into_pyobject(py).unwrap().to_owned().unbind()
                    }))
                })
            }
        }
    }

    fn writelines<'a>(
        &mut self,
        py: Python<'a>,
        lines: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;

        let lines_list: Vec<Vec<u8>> = if let Ok(list) = lines.cast::<pyo3::types::PyList>() {
            list.iter()
                .map(|item| {
                    if is_binary {
                        item.cast::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| value_err("expected bytes"))
                    } else {
                        let py_str = item
                            .cast::<PyString>()
                            .map_err(|_| value_err("expected str"))?;
                        let s = py_str.to_cow().map_err(|_| value_err("invalid UTF-8"))?;
                        Ok(s.as_bytes().to_vec())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            return Err(value_err("expected list"));
        };

        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = file_arc.lock().await;
            for line in &lines_list {
                match &mut *state {
                    FileState::BufferedRead(_) => {
                        return Err(pyo3::exceptions::PyIOError::new_err(
                            "File not open for writing",
                        ));
                    }
                    FileState::Raw(f) => {
                        f.write_all(line)
                            .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    }
                }
            }
            Ok(Python::attach(|py| py.None()))
        })
    }

    #[pyo3(signature = (offset, whence=None))]
    fn seek<'a>(
        &mut self,
        py: Python<'a>,
        offset: i64,
        whence: Option<i32>,
    ) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let whence = whence.unwrap_or(0);
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pos = tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                let seek_from = match whence {
                    0 => {
                        if offset < 0 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "negative offset with SEEK_SET is not allowed",
                            ));
                        }
                        SeekFrom::Start(offset as u64)
                    }
                    1 => SeekFrom::Current(offset),
                    2 => SeekFrom::End(offset),
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "invalid whence",
                        ))
                    }
                };
                state.seek_to(seek_from)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Ok(Python::attach(|py| {
                pos.into_pyobject(py).unwrap().to_owned().unbind()
            }))
        })
    }

    fn tell<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let pos = tokio::task::spawn_blocking(move || {
                let mut state = file_arc.blocking_lock();
                state.position()
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Ok(Python::attach(|py| {
                pos.into_pyobject(py).unwrap().to_owned().unbind()
            }))
        })
    }

    #[pyo3(signature = (size=None))]
    fn truncate<'a>(&mut self, py: Python<'a>, size: Option<u64>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();

            let new_size = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();

                let target_size = if let Some(s) = size {
                    s
                } else {
                    match &mut *state {
                        FileState::BufferedRead(r) => r.stream_position()?,
                        FileState::Raw(f) => f.stream_position()?,
                    }
                };

                match &mut *state {
                    FileState::BufferedRead(r) => {
                        r.get_mut().set_len(target_size)?;
                        r.stream_position()?;
                    }
                    FileState::Raw(f) => {
                        f.set_len(target_size)?;
                    }
                }
                Ok(target_size)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Ok(Python::attach(|py| {
                new_size.into_pyobject(py).unwrap().to_owned().unbind()
            }))
        })
    }

    fn seekable(&self) -> bool {
        true
    }

    fn readable(&self) -> bool {
        self.mode.contains('r') || self.mode.contains('+')
    }

    fn writable(&self) -> bool {
        self.mode.contains('w') || self.mode.contains('a') || self.mode.contains('+')
    }

    fn isatty(&self) -> bool {
        false
    }

    #[getter]
    fn closed(&self) -> bool {
        self.closed
    }

    #[getter]
    fn name(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    #[getter]
    fn mode(&self) -> String {
        self.mode.clone()
    }

    #[getter]
    fn _file(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[getter]
    fn _obj(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn detach<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let path = self.path.to_string_lossy().to_string();
        let mode = self.mode.clone();

        self.file = None;
        self.closed = true;
        self.detached = true;

        let builtins = py.import("builtins")?;
        let file = builtins.call_method1("open", (path, mode))?;
        Ok(file)
    }

    fn fileno<'a>(&mut self, _py: Python<'a>) -> PyResult<i32> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if let Some(file_arc) = &self.file {
            match file_arc.try_lock() {
                Ok(state) => Ok(state.raw_fd()),
                Err(_) => Err(value_err("file is currently locked by another operation")),
            }
        } else {
            Err(value_err("File not open"))
        }
    }

    #[cfg(not(unix))]
    fn fileno(&self) -> PyResult<i32> {
        Err(value_err("fileno not supported on this platform"))
    }

    #[pyo3(signature = (size=None))]
    fn peek<'a>(&mut self, py: Python<'a>, size: Option<i64>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            let size = match size.unwrap_or(1) {
                n if n <= 0 => 1usize,
                n => n as usize,
            };

            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();

                let original_pos = match &mut *state {
                    FileState::BufferedRead(r) => r.stream_position()?,
                    FileState::Raw(f) => f.stream_position()?,
                };

                let mut buffer = vec![0u8; size];
                let bytes_read = match &mut *state {
                    FileState::BufferedRead(r) => r.read(&mut buffer)?,
                    FileState::Raw(f) => f.read(&mut buffer)?,
                };
                buffer.truncate(bytes_read);

                match &mut *state {
                    FileState::BufferedRead(r) => r.seek(SeekFrom::Start(original_pos))?,
                    FileState::Raw(f) => f.seek(SeekFrom::Start(original_pos))?,
                };

                Ok::<Vec<u8>, std::io::Error>(buffer)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?
            .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                if is_binary {
                    Ok(PyBytes::new(py, &buffer).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&buffer);
                    Ok(PyString::new(py, &s).into_any().unbind())
                }
            })
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""));
        }

        if self.file.is_none() {
            return Err(value_err("File not open"));
        }

        let is_binary = self.is_binary;
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();

            let line = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut line = Vec::new();

                let bytes_read = match &mut *state {
                    FileState::BufferedRead(r) => r.read_until(b'\n', &mut line)?,
                    FileState::Raw(f) => {
                        let mut byte = [0u8; 1];
                        let mut count = 0;
                        loop {
                            let n = f.read(&mut byte)?;
                            if n == 0 {
                                break;
                            }
                            line.push(byte[0]);
                            count += 1;
                            if byte[0] == b'\n' {
                                break;
                            }
                        }
                        count
                    }
                };

                if bytes_read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "End of file",
                    ));
                }

                Ok::<Vec<u8>, std::io::Error>(line)
            })
            .await
            .map_err(|e| Python::attach(|py| io_err(py, std::io::Error::other(e))))?;

            match line {
                Ok(line_bytes) => Python::attach(|py| {
                    if is_binary {
                        Ok(PyBytes::new(py, &line_bytes).into_any().unbind())
                    } else {
                        Ok(bytes_to_pystring(py, &line_bytes))
                    }
                }),
                Err(_) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(
                    "End of file",
                )),
            }
        })
    }
}

#[pyfunction]
#[pyo3(signature = (file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, _closefd=true, _opener=None, _loop_=None, _executor=None))]
pub fn open<'a>(
    _py: Python<'a>,
    file: Bound<'a, PyAny>,
    mode: Option<&str>,
    buffering: Option<i32>,
    encoding: Option<String>,
    errors: Option<String>,
    newline: Option<String>,
    _closefd: Option<bool>,
    _opener: Option<Bound<'a, PyAny>>,
    _loop_: Option<Bound<'a, PyAny>>,
    _executor: Option<Bound<'a, PyAny>>,
) -> PyResult<AsyncFile> {
    let mode_str = mode.unwrap_or("r");
    let buffering_val = buffering.unwrap_or(-1);

    let path_str = if let Ok(s) = file.cast::<PyString>() {
        s.str()?.to_string()
    } else if let Ok(has_fspath) = file.hasattr("__fspath__") {
        if has_fspath {
            let fspath_result = file.call_method0("__fspath__")?;
            if let Ok(path_str) = fspath_result.extract::<String>() {
                path_str
            } else {
                return Err(value_err("file must be a string path or PathLike object"));
            }
        } else {
            return Err(value_err("file must be a string path or PathLike object"));
        }
    } else {
        return Err(value_err("file must be a string path or PathLike object"));
    };

    let is_binary = mode_str.contains('b');
    let path = PathBuf::from(&path_str);
    let mode_owned = mode_str.to_string();

    Ok(AsyncFile {
        file: None,
        path,
        mode: mode_owned,
        encoding,
        errors,
        newline,
        buffering: buffering_val,
        is_binary,
        closed: false,
        detached: false,
        write_buffer: Vec::with_capacity(BUFFER_SIZE),
    })
}
