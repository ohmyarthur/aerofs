use crate::utils::{io_err, value_err};
use pyo3::conversion::IntoPyObject;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

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
            let temp_dir = dir.unwrap_or_else(crate::utils::get_temp_dir);
            let file_prefix = prefix.unwrap_or_else(|| "tmp".to_string());
            let file_suffix = suffix.unwrap_or_default();

            let filename = format!("{}{}{}", file_prefix, uuid::Uuid::new_v4(), file_suffix);
            let path = PathBuf::from(temp_dir).join(&filename);

            let mode_str = mode.as_deref().unwrap_or("w+b");
            let mut opts = tokio::fs::OpenOptions::new();
            crate::utils::configure_file_options_async(&mut opts, mode_str, true)?;

            let file = opts
                .open(&path)
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
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

            Ok(Python::attach(|py| py.None()))
        })
    }

    fn close<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        if self.closed {
            return pyo3_async_runtimes::tokio::future_into_py(py, async {
                Ok(Python::attach(|py| py.None()))
            });
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

        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        let is_binary = self.mode.as_ref().map(|m| m.contains('b')).unwrap_or(false);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;

            let data = if let Some(n) = size {
                if n < 0 {
                    let mut buffer = Vec::new();
                    f.read_to_end(&mut buffer)
                        .await
                        .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    buffer
                } else {
                    let mut buffer = vec![0u8; n as usize];
                    let bytes_read = f
                        .read(&mut buffer)
                        .await
                        .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                    buffer.truncate(bytes_read);
                    buffer
                }
            } else {
                let mut buffer = Vec::new();
                f.read_to_end(&mut buffer)
                    .await
                    .map_err(|e| Python::attach(|py| io_err(py, e)))?;
                buffer
            };

            Python::attach(|py| {
                if is_binary {
                    Ok(pyo3::types::PyBytes::new(py, &data).into_any().unbind())
                } else {
                    let s = String::from_utf8_lossy(&data);
                    Ok(PyString::new(py, &s).into_any().unbind())
                }
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
            let mut f = file_arc.lock().await;
            let mut temp_buffer = vec![0u8; buffer_len];

            let bytes_read = f
                .read(&mut temp_buffer)
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                let buffer = buffer_py.bind(py);
                if let Ok(bytearray) = buffer.cast::<pyo3::types::PyByteArray>() {
                    let current_len = bytearray.len();
                    let write_len = std::cmp::min(current_len, bytes_read);

                    unsafe {
                        let ptr = bytearray.as_bytes_mut().as_mut_ptr();
                        std::ptr::copy_nonoverlapping(temp_buffer.as_ptr(), ptr, write_len);
                    }
                    Ok(write_len.into_pyobject(py).unwrap().to_owned().unbind())
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

        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        let bytes = if let Ok(s) = data.cast::<PyString>() {
            s.str()?.to_string().into_bytes()
        } else if let Ok(b) = data.cast::<pyo3::types::PyBytes>() {
            b.as_bytes().to_vec()
        } else {
            return Err(value_err("expected str or bytes"));
        };

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;
            let written = f
                .write(&bytes)
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;
            Ok(Python::attach(|py| {
                written.into_pyobject(py).unwrap().to_owned().unbind()
            }))
        })
    }

    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
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
            let mut f = file_arc.lock().await;
            f.flush()
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;
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

        let file_arc = self
            .file
            .as_ref()
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

            let new_pos = f
                .seek(pos)
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;
            Ok(Python::attach(|py| {
                new_pos.into_pyobject(py).unwrap().to_owned().unbind()
            }))
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
        let file_arc = self
            .file
            .as_ref()
            .ok_or_else(|| value_err("File not open"))?
            .clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut f = file_arc.lock().await;

            let result = if is_binary {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut buf_reader = BufReader::new(&mut *f);
                let mut line_bytes = Vec::new();

                match buf_reader.read_until(b'\n', &mut line_bytes).await {
                    Ok(0) => {
                        return Python::attach(|_py| {
                            Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""))
                        });
                    }
                    Ok(_) => Python::attach(|py| {
                        Ok(pyo3::types::PyBytes::new(py, &line_bytes)
                            .into_any()
                            .unbind())
                    }),
                    Err(e) => Python::attach(|py| Err(io_err(py, e))),
                }
            } else {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut buf_reader = BufReader::new(&mut *f);
                let mut line_str = String::new();

                match buf_reader.read_line(&mut line_str).await {
                    Ok(0) => {
                        return Python::attach(|_py| {
                            Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""))
                        });
                    }
                    Ok(_) => {
                        Python::attach(|py| Ok(PyString::new(py, &line_str).into_any().unbind()))
                    }
                    Err(e) => Python::attach(|py| Err(io_err(py, e))),
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
            let temp_dir = dir.unwrap_or_else(crate::utils::get_temp_dir);
            let dir_prefix = prefix.unwrap_or_else(|| "tmp".to_string());
            let dir_suffix = suffix.unwrap_or_default();

            let dirname = format!("{}{}{}", dir_prefix, uuid::Uuid::new_v4(), dir_suffix);
            let path = PathBuf::from(temp_dir).join(&dirname);

            tokio::fs::create_dir(&path)
                .await
                .map_err(|e| Python::attach(|py| io_err(py, e)))?;

            Python::attach(|py| {
                let mut obj = py_obj.borrow_mut(py);
                obj.path = Some(path);

                if let Some(p) = &obj.path {
                    Ok(PyString::new(py, &p.to_string_lossy()).into_any().unbind())
                } else {
                    Ok(py.None())
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
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn cleanup<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let path = self.path.take();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if let Some(p) = path {
                tokio::fs::remove_dir_all(&p)
                    .await
                    .map_err(|e| Python::attach(|py| io_err(py, e)))?;
            }
            Ok(Python::attach(|py| py.None()))
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
        if let Ok(s) = dir_path.cast::<PyString>() {
            Some(s.str()?.to_string())
        } else if let Ok(has_fspath) = dir_path.hasattr("__fspath__") {
            if has_fspath {
                let fspath_result = dir_path.call_method0("__fspath__")?;
                Some(
                    fspath_result
                        .extract::<String>()
                        .map_err(|_| value_err("dir must be a string path or PathLike object"))?,
                )
            } else {
                return Err(value_err("dir must be a string path or PathLike object"));
            }
        } else {
            return Err(value_err("dir must be a string path or PathLike object"));
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
) -> PyResult<Py<PyAny>> {
    let tempfile_module = py.import("tempfile")?;
    let spooled_class = tempfile_module.getattr("SpooledTemporaryFile")?;

    let kwargs = PyDict::new(py);
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

    let wrap_module = py.import("aerofs.threadpool")?;
    let wrap_func = wrap_module.getattr("wrap")?;

    let wrapped = wrap_func.call1((python_spooled,))?;
    Ok(wrapped.into())
}

pub fn register_tempfile_module(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(named_temporary_file, m)?)?;
    m.add_function(wrap_pyfunction!(temporary_directory, m)?)?;
    m.add_function(wrap_pyfunction!(spooled_temporary_file, m)?)?;
    m.add_class::<AsyncTemporaryFile>()?;
    m.add_class::<AsyncTemporaryDirectory>()?;
    Ok(())
}
