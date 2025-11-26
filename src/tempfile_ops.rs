use pyo3::prelude::*;
use pyo3::types::PyString;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use std::path::PathBuf;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::utils::{io_err, value_err};

#[pyclass]
pub struct AsyncTemporaryFile {
    file: Option<Arc<Mutex<File>>>,
    path: PathBuf,
    delete_on_close: bool,
    closed: bool,
    mode: Option<String>,
    buffering: Option<i32>,
    encoding: Option<String>,
    newline: Option<String>,
    suffix: Option<String>,
    prefix: Option<String>,
    dir: Option<String>,
}

#[pymethods]
impl AsyncTemporaryFile {
    fn __aenter__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let mode = slf.mode.clone();
        let _buffering = slf.buffering;
        let _encoding = slf.encoding.clone();
        let _newline = slf.newline.clone();
        let suffix = slf.suffix.clone();
        let prefix = slf.prefix.clone();
        let dir = slf.dir.clone();
        let _delete_on_close = slf.delete_on_close;
        let py_obj: Py<AsyncTemporaryFile> = slf.into();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let temp_dir = dir.unwrap_or_else(|| crate::utils::get_temp_dir());
            let file_prefix = prefix.unwrap_or_else(|| "tmp".to_string());
            let file_suffix = suffix.unwrap_or_else(|| "".to_string());
            
            let filename = format!("{}{}{}", file_prefix, uuid::Uuid::new_v4(), file_suffix);
            let path = PathBuf::from(temp_dir).join(&filename);
            
            let mode_str = mode.as_deref().unwrap_or("w+b");
            let mut opts = tokio::fs::OpenOptions::new();
            crate::utils::configure_file_options_async(&mut opts, mode_str, true)?;
            
            let file = opts.open(&path).await
                .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.file = Some(Arc::new(Mutex::new(file)));
                obj.path = path;
                Ok(py_obj.clone_ref(py))
            })
        })
    }
    
    fn __aexit__<'a>(
        mut slf: PyRefMut<'a, Self>,
        py: Python<'a>,
        _exc_type: Bound<'a, PyAny>,
        _exc_val: Bound<'a, PyAny>,
        _exc_tb: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let path = slf.path.clone();
        let file = slf.file.take();
        slf.closed = true;
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(file_arc) = file {
                let mut f = file_arc.lock().await;
                f.flush().await.ok();
                drop(f);
            }
            
            tokio::fs::remove_file(&path).await.ok();
            
            Ok(Python::with_gil(|py| false.into_py(py)))
        })
    }
    
    fn close<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return pyo3_async_runtimes::tokio::future_into_py(py, async { Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py))) });
        }
        
        let file = self.file.take();
        let path = self.path.clone();
        let delete = self.delete_on_close;
        self.closed = true;
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(file_arc) = file {
                let mut f = file_arc.lock().await;
                f.flush().await.ok();
                drop(f);
            }
            
            if delete {
                tokio::fs::remove_file(&path).await.ok();
            }
            
            Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
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
        
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        let is_binary = self.mode.as_ref().map(|m| m.contains('b')).unwrap_or(false);
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;
            
            let data = if let Some(n) = size {
                if n < 0 {
                    let mut buffer = Vec::new();
                    f.read_to_end(&mut buffer).await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                    buffer
                } else {
                    let mut buffer = vec![0u8; n as usize];
                    let bytes_read = f.read(&mut buffer).await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                    buffer.truncate(bytes_read);
                    buffer
                }
            } else {
                let mut buffer = Vec::new();
                f.read_to_end(&mut buffer).await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
                buffer
            };
            
            Python::with_gil(|py| {
                if is_binary {
                    Ok(pyo3::types::PyBytes::new_bound(py, &data).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&data);
                    Ok(PyString::new_bound(py, &s).into_any().unbind())
                }
            })
        })
    }
    
    fn readinto<'a>(&mut self, py: Python<'a>, _buffer: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::with_gil(|py| {
                Ok(0.into_py(py))  // Return 0 bytes read as placeholder
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
        
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        let bytes = if let Ok(s) = data.downcast::<PyString>() {
            s.str()?.to_string().into_bytes()
        } else if let Ok(b) = data.downcast::<pyo3::types::PyBytes>() {
            b.as_bytes().to_vec()
        } else {
            return Err(value_err("expected str or bytes"));
        };
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;
            let written = f.write(&bytes).await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            Ok(Python::with_gil(|py| written.into_py(py)))
        })
    }
    
    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
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
            let mut f = file_arc.lock().await;
            f.flush().await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            Ok(Python::with_gil(|py| py.None()))
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
        
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        let whence = whence.unwrap_or(0);
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;
            
            let pos = match whence {
                0 => SeekFrom::Start(offset as u64),
                1 => SeekFrom::Current(offset),
                2 => SeekFrom::End(offset),
                _ => return Err(value_err("invalid whence value")),
            };
            
            let new_pos = f.seek(pos).await.map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            Ok(Python::with_gil(|py| new_pos.into_py(py)))
        })
    }
    
    #[getter]
    fn name(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
    
    #[getter]
    fn _file(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
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
        
        let is_binary = self.mode.as_deref().unwrap_or("r").contains('b');
        let file_arc = self.file.as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;
            
            let result = if is_binary {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut buf_reader = BufReader::new(&mut *f);
                let mut line_buf = Vec::new();
                
                match buf_reader.read_until(b'\n', &mut line_buf).await {
                    Ok(0) => {
                        return Python::with_gil(|_py| {
                            Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""))
                        });
                    }
                    Ok(_) => {
                        Python::with_gil(|py| {
                            Ok(pyo3::types::PyBytes::new_bound(py, &line_buf).into_any().unbind())
                        })
                    }
                    Err(e) => {
                        Python::with_gil(|py| {
                            Err(io_err(py, e))
                        })
                    }
                }
            } else {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut buf_reader = BufReader::new(&mut *f);
                let mut line_str = String::new();
                
                match buf_reader.read_line(&mut line_str).await {
                    Ok(0) => {
                        return Python::with_gil(|_py| {
                            Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""))
                        });
                    }
                    Ok(_) => {
                        Python::with_gil(|py| {
                            Ok(PyString::new_bound(py, &line_str).into_any().unbind())
                        })
                    }
                    Err(e) => {
                        Python::with_gil(|py| {
                            Err(io_err(py, e))
                        })
                    }
                }
            };
            
            result
        })
    }
}

#[pyclass]
pub struct AsyncTemporaryDirectory {
    path: Option<PathBuf>,
    prefix: Option<String>,
    suffix: Option<String>,
    dir: Option<String>,
}

#[pymethods]
impl AsyncTemporaryDirectory {
    fn __aenter__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let prefix = slf.prefix.clone();
        let suffix = slf.suffix.clone();
        let dir = slf.dir.clone();
        let py_obj: Py<AsyncTemporaryDirectory> = slf.into();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let temp_dir = dir.unwrap_or_else(|| crate::utils::get_temp_dir());
            let dir_prefix = prefix.unwrap_or_else(|| "tmp".to_string());
            let dir_suffix = suffix.unwrap_or_else(|| "".to_string());
            
            let dirname = format!("{}{}{}", dir_prefix, uuid::Uuid::new_v4(), dir_suffix);
            let path = PathBuf::from(temp_dir).join(&dirname);
            
            tokio::fs::create_dir(&path).await
                .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.path = Some(path);
                
                if let Some(p) = &obj.path {
                    Ok(PyString::new_bound(py, &p.to_string_lossy()).into_any().unbind())
                } else {
                    Ok(<() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py))
                }
            })
        })
    }
    
    fn __aexit__<'a>(
        mut slf: PyRefMut<'a, Self>,
        py: Python<'a>,
        _exc_type: Bound<'a, PyAny>,
        _exc_val: Bound<'a, PyAny>,
        _exc_tb: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let path = slf.path.take();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(p) = path {
                tokio::fs::remove_dir_all(&p).await.ok();
            }
            Ok(Python::with_gil(|py| false.into_py(py)))
        })
    }
    
    fn cleanup<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let path = self.path.take();
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(p) = path {
                tokio::fs::remove_dir_all(&p).await
                    .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            }
            Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
        })
    }
    
    #[getter]
    fn name(&self) -> Option<String> {
        self.path.as_ref().map(|p| p.to_string_lossy().to_string())
    }
}

#[pyfunction]
#[pyo3(signature = (mode="w+b", buffering=-1, encoding=None, newline=None, suffix=None, prefix=None, dir=None, delete=true, *, delete_on_close=None))]
pub fn named_temporary_file<'a>(
    _py: Python<'a>,
    mode: Option<&str>,
    buffering: Option<i32>,
    encoding: Option<String>,
    newline: Option<String>,
    suffix: Option<String>,
    prefix: Option<String>,
    dir: Option<String>,
    delete: Option<bool>,
    delete_on_close: Option<bool>,
) -> PyResult<AsyncTemporaryFile> {
    let delete_flag = delete_on_close.or(delete).unwrap_or(true);
    
    Ok(AsyncTemporaryFile {
        file: None,
        path: PathBuf::from(crate::utils::get_temp_dir()).join("placeholder"),
        delete_on_close: delete_flag,
        closed: false,
        mode: mode.map(|s| s.to_string()),
        buffering,
        encoding,
        newline,
        suffix,
        prefix,
        dir,
    })
}

#[pyfunction]
#[pyo3(signature = (prefix=None, suffix=None, dir=None))]
pub fn temporary_directory<'a>(
    _py: Python<'a>,
    prefix: Option<String>,
    suffix: Option<String>,
    dir: Option<Bound<'a, PyAny>>,
) -> PyResult<AsyncTemporaryDirectory> {
    let dir_str = if let Some(dir_path) = dir {
        // Convert PathLike objects to string
        if let Ok(s) = dir_path.downcast::<PyString>() {
            Some(s.str()?.to_string())
        } else {
            if let Ok(has_fspath) = dir_path.hasattr("__fspath__") {
                if has_fspath {
                    let fspath_result = dir_path.call_method0("__fspath__")?;
                    Some(fspath_result.extract::<String>().map_err(|_| value_err("dir must be a string path or PathLike object"))?)
                } else {
                    return Err(value_err("dir must be a string path or PathLike object"));
                }
            } else {
                return Err(value_err("dir must be a string path or PathLike object"));
            }
        }
    } else {
        None
    };
    
    Ok(AsyncTemporaryDirectory {
        path: None,
        prefix,
        suffix,
        dir: dir_str,
    })
}

#[pyclass]
pub struct AsyncSpooledTemporaryFile {
    file: Option<Py<AsyncTemporaryFile>>,
    #[allow(dead_code)]
    max_size: usize,
    mode: Option<String>,
    suffix: Option<String>,
    prefix: Option<String>,
    dir: Option<String>,
    buffering: Option<i32>,
    encoding: Option<String>,
    newline: Option<String>,
}

#[pymethods]
impl AsyncSpooledTemporaryFile {
    fn __aenter__<'a>(slf: PyRefMut<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let py_obj: Py<AsyncSpooledTemporaryFile> = slf.into();
        
        let (mode, suffix, prefix, dir, buffering, encoding, newline) = Python::with_gil(|py| {
            let obj = py_obj.borrow(py);
            (
                obj.mode.clone(),
                obj.suffix.clone(),
                obj.prefix.clone(),
                obj.dir.clone(),
                obj.buffering,
                obj.encoding.clone(),
                obj.newline.clone(),
            )
        });
        
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let temp_dir = dir.unwrap_or_else(|| crate::utils::get_temp_dir());
            let file_prefix = prefix.unwrap_or_else(|| "tmp".to_string());
            let file_suffix = suffix.unwrap_or_else(|| "".to_string());
            let mode_str = mode.as_deref().unwrap_or("w+b");
            
            let filename = format!("{}{}{}", file_prefix, uuid::Uuid::new_v4(), file_suffix);
            let path = PathBuf::from(temp_dir.clone()).join(&filename);
            
            let mut opts = tokio::fs::OpenOptions::new();
            crate::utils::configure_file_options_async(&mut opts, mode_str, true)?;
            
            let file = opts.open(&path).await
                .map_err(|e| Python::with_gil(|py| io_err(py, e)))?;
            
            Python::with_gil(|py| {
                let temp_file = AsyncTemporaryFile {
                    file: Some(Arc::new(Mutex::new(file))),
                    path,
                    delete_on_close: true,
                    closed: false,
                    mode: mode.clone(),
                    buffering,
                    encoding,
                    newline,
                    suffix: Some(file_suffix.clone()),
                    prefix: Some(file_prefix.clone()),
                    dir: Some(temp_dir.clone()),
                };
                
                let py_temp_file = Py::new(py, temp_file)
                    .map_err(|e| PyErr::from(e))?;
                
                let mut obj = py_obj.borrow_mut(py);
                obj.file = Some(py_temp_file);
                Ok(py_obj.clone_ref(py))
            })
        })
    }
    
    fn __aexit__<'a>(
        _slf: PyRefMut<'a, Self>,
        py: Python<'a>,
        _exc_type: Bound<'a, PyAny>,
        _exc_val: Bound<'a, PyAny>,
        _exc_tb: Bound<'a, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(Python::with_gil(|py| false.into_py(py)))
        })
    }
    
    #[pyo3(signature = (size=None))]
    fn read<'a>(&mut self, py: Python<'a>, size: Option<i64>) -> PyResult<Bound<'a, PyAny>> {
        if let Some(ref py_file) = self.file {
            let mut file = py_file.borrow_mut(py);
            file.read(py, size)
        } else {
            Err(value_err("No underlying file"))
        }
    }
    
    fn write<'a>(&mut self, py: Python<'a>, data: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
        if let Some(ref py_file) = self.file {
            let mut file = py_file.borrow_mut(py);
            file.write(py, data)
        } else {
            Err(value_err("No underlying file"))
        }
    }
    
    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if let Some(ref py_file) = self.file {
            let mut file = py_file.borrow_mut(py);
            file.flush(py)
        } else {
            Err(value_err("No underlying file"))
        }
    }
    
    #[pyo3(signature = (offset, whence=None))]
    fn seek<'a>(&mut self, py: Python<'a>, offset: i64, whence: Option<i32>) -> PyResult<Bound<'a, PyAny>> {
        if let Some(ref py_file) = self.file {
            let mut file = py_file.borrow_mut(py);
            file.seek(py, offset, whence)
        } else {
            Err(value_err("No underlying file"))
        }
    }
    
    #[getter]
    fn name(&self) -> Option<String> {
        Python::with_gil(|py| {
            self.file.as_ref().and_then(|py_file| {
                let file = py_file.borrow(py);
                Some(file.name())
            })
        })
    }
    
    #[getter]
    fn delete(&self) -> bool {
        Python::with_gil(|py| {
            self.file.as_ref().map(|py_file| {
                let file = py_file.borrow(py);
                file.delete_on_close
            }).unwrap_or(true)
        })
    }
    
    #[getter]
    fn _file(&self, py: Python<'_>) -> Option<Py<AsyncTemporaryFile>> {
        self.file.as_ref().map(|f| f.clone_ref(py))
    }
}

#[pyfunction]
#[pyo3(signature = (max_size=0, mode="w+b", buffering=-1, encoding=None, newline=None, suffix=None, prefix=None, dir=None))]
pub fn spooled_temporary_file<'a>(
    py: Python<'a>,
    max_size: Option<i64>,
    mode: Option<&str>,
    buffering: Option<i32>,
    encoding: Option<String>,
    newline: Option<String>,
    suffix: Option<String>,
    prefix: Option<String>,
    dir: Option<String>,
) -> PyResult<PyObject> {
    let tempfile_module = py.import_bound("tempfile")
        .map_err(|e| PyErr::from(e))?;
    
    let spooled_class = tempfile_module.getattr("SpooledTemporaryFile")
        .map_err(|e| PyErr::from(e))?;
    
    let kwargs = pyo3::types::PyDict::new_bound(py);
    kwargs.set_item("max_size", max_size.unwrap_or(0))?;
    kwargs.set_item("mode", mode.unwrap_or("w+b"))?;
    kwargs.set_item("buffering", buffering.unwrap_or(-1))?;
    if let Some(ref enc) = encoding {
        kwargs.set_item("encoding", enc)?;
    }
    if let Some(ref nl) = newline {
        kwargs.set_item("newline", nl)?;
    }
    if let Some(ref sf) = suffix {
        kwargs.set_item("suffix", sf)?;
    }
    if let Some(ref pf) = prefix {
        kwargs.set_item("prefix", pf)?;
    }
    if let Some(ref d) = dir {
        kwargs.set_item("dir", d)?;
    }
    
    let python_spooled = spooled_class.call((), Some(&kwargs))?;
    
    let wrap_module = py.import_bound("aerofs.threadpool")
        .map_err(|e| PyErr::from(e))?;
    let wrap_func = wrap_module.getattr("wrap")
        .map_err(|e| PyErr::from(e))?;
    
    let wrapped = wrap_func.call1((python_spooled,))?;
    Ok(wrapped.into())
}

pub fn register_tempfile_module(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(named_temporary_file, m)?)?;
    m.add_function(wrap_pyfunction!(temporary_directory, m)?)?;
    m.add_function(wrap_pyfunction!(spooled_temporary_file, m)?)?;
    m.add_class::<AsyncTemporaryFile>()?;
    m.add_class::<AsyncTemporaryDirectory>()?;
    m.add_class::<AsyncSpooledTemporaryFile>()?;
    Ok(())
}
