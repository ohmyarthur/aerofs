use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString, PyList, PyByteArray, PyBytesMethods};
use pyo3::ffi;
use std::fs::File;
use std::io::{Read, Write, Seek, SeekFrom, BufReader, BufRead};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::os::unix::io::AsRawFd;
use crate::utils::{io_err, value_err};


const BUFFER_SIZE: usize = 131072;

enum FileState {
    BufferedRead(BufReader<File>),
    Raw(File),
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
    fn __aenter__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if slf.file.is_some() {
            let py_obj: Py<AsyncFile> = slf.into();
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok(py_obj)
            });
        }
        
        let path = slf.path.clone();
        let mode = slf.mode.clone();
        let py_obj: Py<AsyncFile> = slf.into();
        
        let path_clone = path.clone();
        let mode_clone = mode.clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_state = tokio::task::spawn_blocking(move || {
                let mut options = std::fs::OpenOptions::new();
                
                if mode_clone.contains('r') {
                    options.read(true);
                    if mode_clone.contains('+') {
                        options.write(true);
                    }
                }
                if mode_clone.contains('w') {
                    options.write(true).create(true).truncate(true);
                    if mode_clone.contains('+') {
                        options.read(true);
                    }
                }
                if mode_clone.contains('a') {
                    options.write(true).create(true).append(true);
                    if mode_clone.contains('+') {
                        options.read(true);
                    }
                }
                if mode_clone.contains('x') {
                    options.write(true).create_new(true);
                }
                
                let file = options.open(&path_clone)?;
                
                if mode_clone == "r" || mode_clone == "rb" {
                    Ok(FileState::BufferedRead(BufReader::with_capacity(BUFFER_SIZE, file)))
                } else {
                    Ok(FileState::Raw(file))
                }
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e: std::io::Error| Python::with_gil(|_py| {
                  let errno = e.raw_os_error();
                  let msg = match e.kind() {
                      std::io::ErrorKind::NotFound => format!("No such file or directory: '{}'", path.display()),
                      std::io::ErrorKind::PermissionDenied => format!("Permission denied: '{}'", path.display()),
                      _ => format!("{}: '{}'", e, path.display()),
                  };
                  pyo3::exceptions::PyOSError::new_err((errno, msg))
              }))?;
            
            Python::with_gil(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.file = Some(Arc::new(Mutex::new(file_state)));
                Ok(py_obj.clone_ref(py))
            })
        })
    }
    
    fn __await__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if slf.file.is_some() {
            let py_obj: Py<AsyncFile> = slf.into();
            let future = pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok(py_obj)
            })?;
            return future.call_method0("__await__");
        }
        
        let path = slf.path.clone();
        let mode = slf.mode.clone();
        let py_obj: Py<AsyncFile> = slf.into();
        
        let path_clone = path.clone();
        let mode_clone = mode.clone();
        
        let future = pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_state = tokio::task::spawn_blocking(move || {
                let mut options = std::fs::OpenOptions::new();
                
                if mode_clone.contains('r') {
                    options.read(true);
                    if mode_clone.contains('+') {
                        options.write(true);
                    }
                }
                if mode_clone.contains('w') {
                    options.write(true).create(true).truncate(true);
                    if mode_clone.contains('+') {
                        options.read(true);
                    }
                }
                if mode_clone.contains('a') {
                    options.write(true).create(true).append(true);
                    if mode_clone.contains('+') {
                        options.read(true);
                    }
                }
                if mode_clone.contains('x') {
                    options.write(true).create_new(true);
                }
                
                let file = options.open(&path_clone)?;
                
                if mode_clone == "r" || mode_clone == "rb" {
                    Ok(FileState::BufferedRead(BufReader::with_capacity(BUFFER_SIZE, file)))
                } else {
                    Ok(FileState::Raw(file))
                }
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e: std::io::Error| Python::with_gil(|_py| {
                  let errno = e.raw_os_error();
                  let msg = match e.kind() {
                      std::io::ErrorKind::NotFound => format!("No such file or directory: '{}'", path.display()),
                      std::io::ErrorKind::PermissionDenied => format!("Permission denied: '{}'", path.display()),
                      _ => format!("{}: '{}'", e, path.display()),
                  };
                  pyo3::exceptions::PyOSError::new_err((errno, msg))
              }))?;
            
            Python::with_gil(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.file = Some(Arc::new(Mutex::new(file_state)));
                Ok(py_obj.clone_ref(py))
            })
        })?;
        
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
                let file_arc_clone = file_arc.clone();
                tokio::task::spawn_blocking(move || {
                    let mut state = file_arc_clone.blocking_lock();
                    if !buffer.is_empty() {
                        match &mut *state {
                            FileState::BufferedRead(r) => {
                                let f = r.get_mut();
                                f.write_all(&buffer).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                            }
                            FileState::Raw(f) => {
                                f.write_all(&buffer).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                            }
                        }
                    }
                    match &mut *state {
                        FileState::BufferedRead(r) => {
                            r.get_mut().flush().ok();
                        }
                        FileState::Raw(f) => {
                            f.flush().ok();
                        }
                    }
                    Ok::<(), PyErr>(())
                }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))??;
            }
            Ok(Python::with_gil(|py| false.into_py(py)))
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
                let file_arc_clone = file_arc.clone();
                tokio::task::spawn_blocking(move || {
                    let mut state = file_arc_clone.blocking_lock();
                    if !buffer.is_empty() {
                        match &mut *state {
                            FileState::BufferedRead(r) => {
                                let f = r.get_mut();
                                f.write_all(&buffer).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                            }
                            FileState::Raw(f) => {
                                f.write_all(&buffer).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                            }
                        }
                    }
                    match &mut *state {
                        FileState::BufferedRead(r) => {
                            r.get_mut().flush().map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                        }
                        FileState::Raw(f) => {
                            f.flush().map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                        }
                    }
                    Ok::<(), PyErr>(())
                }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))??;
            }
            Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
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
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                if !buffer.is_empty() {
                    match &mut *state {
                        FileState::BufferedRead(r) => {
                             let f = r.get_mut();
                             f.write_all(&buffer)?;
                             f.flush()?;
                        }
                        FileState::Raw(f) => {
                            f.write_all(&buffer)?;
                            f.flush()?;
                        }
                    }
                } else {
                     match &mut *state {
                        FileState::BufferedRead(r) => {
                             r.get_mut().flush()?;
                        }
                        FileState::Raw(f) => {
                            f.flush()?;
                        }
                     }
                }
                Ok::<(), std::io::Error>(())
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Ok(Python::with_gil(|py| py.None()))
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
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        let size_opt = size;
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(n) = size_opt {
                if n >= 0 {
                    let size = n as usize;
                    
                    let (bytes_obj, buffer_ptr) = Python::with_gil(|py| {
                        unsafe {
                            let ptr = ffi::PyBytes_FromStringAndSize(std::ptr::null(), size as isize);
                            if ptr.is_null() {
                                return Err(PyErr::fetch(py));
                            }
                            let buffer = ffi::PyBytes_AsString(ptr);
                            let obj = Py::from_owned_ptr(py, ptr);
                            Ok((obj, buffer))
                        }
                    })?;
                    
                    // Safety: We hold `bytes_obj` in the async frame (via `bytes_obj` variable).
                    
                    let buffer_ptr_int = buffer_ptr as usize;
                    let file_arc_clone = file_arc.clone();
                    
                    let bytes_read = tokio::task::spawn_blocking(move || {
                        let mut state = file_arc_clone.blocking_lock();
                        let buffer_slice = unsafe { std::slice::from_raw_parts_mut(buffer_ptr_int as *mut u8, size) };
                        
                        match &mut *state {
                            FileState::BufferedRead(r) => r.read(buffer_slice),
                            FileState::Raw(f) => f.read(buffer_slice),
                        }
                    }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
                      .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                    
                    if bytes_read < size {
                        return Python::with_gil(|py| {
                            
                            
                            let _bytes_ref = bytes_obj.bind(py);
                            let full_slice = unsafe { std::slice::from_raw_parts(buffer_ptr_int as *const u8, size) };
                            let data_slice = &full_slice[0..bytes_read];
                            
                            if is_binary {
                                Ok(PyBytes::new_bound(py, data_slice).into_any().unbind())
                            } else {
                                let s = std::str::from_utf8(data_slice)
                                    .map_err(|e| pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string()))?;
                                Ok(PyString::new_bound(py, s).into_any().unbind())
                            }
                        });
                    } else {
                        return Python::with_gil(|py| {
                            if is_binary {
                                Ok(bytes_obj.into_any())
                            } else {
                                let bytes_ref = bytes_obj.bind(py);
                                let s = std::str::from_utf8(PyBytesMethods::as_bytes(bytes_ref))
                                    .map_err(|e| pyo3::exceptions::PyUnicodeDecodeError::new_err(e.to_string()))?;
                                Ok(PyString::new_bound(py, s).into_any().unbind())
                            }
                        });
                    }
                }
            }
            
            let file_arc_clone = file_arc.clone();
            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut buffer = Vec::with_capacity(BUFFER_SIZE);
                match &mut *state {
                    FileState::BufferedRead(r) => {
                        r.read_to_end(&mut buffer)?;
                    }
                    FileState::Raw(f) => {
                        f.read_to_end(&mut buffer)?;
                    }
                }
                Ok::<Vec<u8>, std::io::Error>(buffer)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                if is_binary {
                    Ok(PyBytes::new_bound(py, &buffer).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&buffer);
                    Ok(PyString::new_bound(py, &s).into_any().unbind())
                }
            })
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
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut buffer = vec![0u8; size];
                let bytes_read = match &mut *state {
                    FileState::BufferedRead(r) => r.read(&mut buffer)?,
                    FileState::Raw(f) => f.read(&mut buffer)?,
                };
                buffer.truncate(bytes_read);
                Ok::<Vec<u8>, std::io::Error>(buffer)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                if is_binary {
                    Ok(PyBytes::new_bound(py, &buffer).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&buffer);
                    Ok(PyString::new_bound(py, &s).into_any().unbind())
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
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            
            let file_arc_clone = file_arc.clone();
            let line = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let mut line = Vec::new();
                
                let _bytes_read = if let Some(n) = size {
                    if n <= 0 {
                        match &mut *state {
                            FileState::BufferedRead(r) => {
                                r.read_until(b'\n', &mut line)?
                            }
                            FileState::Raw(f) => {
                                let mut byte = [0u8; 1];
                                loop {
                                    let n = f.read(&mut byte)?;
                                    if n == 0 { break; }
                                    line.push(byte[0]);
                                    if byte[0] == b'\n' { break; }
                                }
                                line.len()
                            }
                        }
                    } else {
                        for _ in 0..n {
                            let mut byte = [0u8; 1];
                            let n_read = match &mut *state {
                                FileState::BufferedRead(r) => r.read(&mut byte)?,
                                FileState::Raw(f) => f.read(&mut byte)?,
                            };
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
                    match &mut *state {
                        FileState::BufferedRead(r) => {
                            r.read_until(b'\n', &mut line)?
                        }
                        FileState::Raw(f) => {
                            let mut byte = [0u8; 1];
                            loop {
                                let n = f.read(&mut byte)?;
                                if n == 0 {
                                    break;
                                }
                                line.push(byte[0]);
                                if byte[0] == b'\n' {
                                    break;
                                }
                            }
                            line.len()
                        }
                    }
                };
                Ok::<Vec<u8>, std::io::Error>(line)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
            if is_binary {
                    Ok(PyBytes::new_bound(py, &line).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&line);
                    Ok(PyString::new_bound(py, &s).into_any().unbind())
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
        let file_arc = self.file.as_ref()
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
                                if n == 0 { break; }
                                line_buffer.push(byte[0]);
                                count += 1;
                                if byte[0] == b'\n' { break; }
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
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                let list = PyList::empty_bound(py);
                for line in lines {
                    if is_binary {
                        list.append(PyBytes::new_bound(py, &line))?;
                    } else {
                        let s = String::from_utf8_lossy(&line);
                        list.append(PyString::new_bound(py, &s))?;
                    }
                }
                Ok(list.into_any().unbind())
            })
        })
    }
    
    fn readinto<'a>(&mut self, py: Python<'a>, buffer: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }
        
        let buffer_len = buffer.len()?;
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
            
        
        let buffer_ptr = if let Ok(bytearray) = buffer.downcast::<PyByteArray>() {
            unsafe { bytearray.as_bytes_mut().as_mut_ptr() }
        } else {
             return Err(value_err("Only bytearray is supported for fast readinto currently"));
        };
        
        let buffer_ptr_int = buffer_ptr as usize;
        let file_arc_clone = file_arc.clone();
        
        let buffer_py = buffer.unbind();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let bytes_read = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let buffer_slice = unsafe { std::slice::from_raw_parts_mut(buffer_ptr_int as *mut u8, buffer_len) };
                
                match &mut *state {
                    FileState::BufferedRead(r) => r.read(buffer_slice),
                    FileState::Raw(f) => f.read(buffer_slice),
                }
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            let _ = buffer_py; 
            
            Ok(Python::with_gil(|py| bytes_read.into_py(py)))
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
        
        let bytes = if is_binary {
            data.downcast::<PyBytes>()
                .map_err(|_| value_err("expected bytes"))?
                .as_bytes()
                .to_vec()
        } else {
            let s = data.downcast::<PyString>()
                .map_err(|_| value_err("expected str"))?
                .str()?
                .to_string();
            s.into_bytes()
        };
        
        let bytes_len = bytes.len();
        
        if self.buffering == 0 {
            let file_arc = self.file.as_ref()
                .ok_or_else(|| value_err("File not open"))?
                .clone();
            
            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                let bytes_vec = bytes; // Move bytes into the async block
                let bytes_written = tokio::task::spawn_blocking(move || {
                    let mut state = file_arc.blocking_lock();
                    match &mut *state {
                        FileState::BufferedRead(_) => {
                            Err(pyo3::exceptions::PyIOError::new_err("File not open for writing"))
                        }
                        FileState::Raw(f) => {
                            f.write(&bytes_vec).map_err(|e| Python::with_gil(|py| io_err(py, e)))
                        }
                    }
                }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))??;
                
                Ok(Python::with_gil(|py| bytes_written.into_py(py)))
            })
        } else {
            self.write_buffer.extend_from_slice(&bytes);
            
            if self.write_buffer.len() >= BUFFER_SIZE {
                let buffer = std::mem::replace(&mut self.write_buffer, Vec::with_capacity(BUFFER_SIZE));
                let file_arc = self.file.as_ref()
                    .ok_or_else(|| value_err("File not open"))?
                    .clone();
                
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let mut state = file_arc.lock().await;
                    match &mut *state {
                        FileState::BufferedRead(_) => {
                            return Err(pyo3::exceptions::PyIOError::new_err("File not open for writing"));
                        }
                        FileState::Raw(f) => {
                            f.write_all(&buffer).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                        }
                    }
                    Ok(Python::with_gil(|py| bytes_len.into_py(py)))
                })
            } else {
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    Ok(Python::with_gil(|py| bytes_len.into_py(py)))
                })
            }
        }
    }
    
    fn writelines<'a>(&mut self, py: Python<'a>, lines: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }
        
        let is_binary = self.is_binary;
        
        let lines_list: Vec<Vec<u8>> = if let Ok(list) = lines.downcast::<pyo3::types::PyList>() {
            list.iter()
                .map(|item| {
                    if is_binary {
                        item.downcast::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| value_err("expected bytes"))
                    } else {
                        let py_str = item.downcast::<PyString>()
                            .map_err(|_| value_err("expected str"))?;
                        let s = py_str.to_cow().map_err(|_| value_err("invalid UTF-8"))?;
                        Ok(s.as_bytes().to_vec())
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            return Err(value_err("expected list"));
        };
        
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = file_arc.lock().await;
            for line in &lines_list {
                match &mut *state {
                    FileState::BufferedRead(_) => {
                        return Err(pyo3::exceptions::PyIOError::new_err("File not open for writing"));
                    }
                    FileState::Raw(f) => {
                        f.write_all(line).map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                    }
                }
            }
            Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
        })
    }
    
    #[pyo3(signature = (offset, whence=None))]
    fn seek<'a>(&mut self, py: Python<'a>, offset: i64, whence: Option<i32>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }
        
        let whence = whence.unwrap_or(0);
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            let pos = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                let seek_from = match whence {
                    0 => SeekFrom::Start(offset as u64),
                    1 => SeekFrom::Current(offset),
                    2 => SeekFrom::End(offset),
                    _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid whence")),
                };
                
                match &mut *state {
                    FileState::BufferedRead(r) => r.seek(seek_from),
                    FileState::Raw(f) => f.seek(seek_from),
                }
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Ok(Python::with_gil(|py| pos.into_py(py)))
        })
    }
    
    fn tell<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }
        
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            let pos = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                match &mut *state {
                    FileState::BufferedRead(r) => r.stream_position(),
                    FileState::Raw(f) => f.stream_position(),
                }
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Ok(Python::with_gil(|py| pos.into_py(py)))
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
        
        let file_arc = self.file.as_ref()
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
                        // But we can't easily get mutable reference to inner file from BufReader?
                        // We can use r.get_mut().
                        r.get_mut().set_len(target_size)?;
                        // Invalidate buffer by seeking to current position
                        r.seek(SeekFrom::Current(0))?;
                    }
                    FileState::Raw(f) => {
                        f.set_len(target_size)?;
                    }
                }
                Ok(target_size)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Ok(Python::with_gil(|py| new_size.into_py(py)))
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
        // Return self as the underlying file for compatibility
        slf
    }
    
    #[getter]
    fn _obj(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        // Return self as the underlying file object for aiofiles compatibility
        slf
    }
    
    fn detach<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if self.file.is_none() {
            return Err(value_err("File not open"));
        }
        
        // Create a new Python file object for the same path
        // This allows the test to use raw_file.read() after detach
        let path = self.path.to_string_lossy().to_string();
        let mode = self.mode.clone();
        
        // Mark file as closed/detached so future operations fail
        self.file = None;
        self.closed = true;
        self.detached = true;
        
        // Return a Python file object
        let builtins = py.import_bound("builtins")?;
        let file = builtins.call_method1("open", (path, mode))?;
        Ok(file)
    }
    
    fn fileno<'a>(&mut self, _py: Python<'a>) -> PyResult<i32> {
        if self.closed {
            return Err(value_err("I/O operation on closed file"));
        }
        
        if let Some(file_arc) = &self.file {
            // We need to lock to get the file.
            // But fileno is synchronous in Python usually?
            // Wait, this is `fileno()`. It returns an integer.
            // We can block here since it's just getting an FD.
            let state = file_arc.blocking_lock();
            let fd = match &*state {
                FileState::BufferedRead(r) => r.get_ref().as_raw_fd(),
                FileState::Raw(f) => f.as_raw_fd(),
            };
            Ok(fd)
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
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file_arc_clone = file_arc.clone();
            let size = size.unwrap_or(1) as usize; // Convert Option<i64> to usize, default to 1
            
            let buffer = tokio::task::spawn_blocking(move || {
                let mut state = file_arc_clone.blocking_lock();
                
                // Save current position
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
                
                // Restore position
                match &mut *state {
                    FileState::BufferedRead(r) => r.seek(SeekFrom::Start(original_pos))?,
                    FileState::Raw(f) => f.seek(SeekFrom::Start(original_pos))?,
                };
                
                Ok::<Vec<u8>, std::io::Error>(buffer)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?
              .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                if is_binary {
                    Ok(PyBytes::new_bound(py, &buffer).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&buffer);
                    Ok(PyString::new_bound(py, &s).into_any().unbind())
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
        let file_arc = self.file.as_ref()
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
                            if n == 0 { break; }
                            line.push(byte[0]);
                            count += 1;
                            if byte[0] == b'\n' { break; }
                        }
                        count
                    }
                };
                
                if bytes_read == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "End of file"));
                }
                
                Ok::<Vec<u8>, std::io::Error>(line)
            }).await.map_err(|e| Python::with_gil(|py| io_err(py, std::io::Error::new(std::io::ErrorKind::Other, e))))?;
            
            match line {
                Ok(line_bytes) => {
                    Python::with_gil(|py| {
                        if is_binary {
                            Ok(PyBytes::new_bound(py, &line_bytes).into_any().unbind())
                        } else {
                            let s = String::from_utf8_lossy(&line_bytes);
                            Ok(PyString::new_bound(py, &s).into_any().unbind())
                        }
                    })
                },
                Err(_) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err("End of file"))
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
    
    let path_str = if let Ok(s) = file.downcast::<PyString>() {
        s.str()?.to_string()
    } else {
        // Try to convert PathLike objects to string
        if let Ok(has_fspath) = file.hasattr("__fspath__") {
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
        }
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
