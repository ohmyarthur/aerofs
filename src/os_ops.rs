use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};
use tokio::fs;
use std::path::PathBuf;
use crate::utils::{os_err, value_err};

/// Helper function to convert PathLike objects to strings
fn path_to_string(path: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = path.downcast::<PyString>() {
        Ok(s.str()?.to_string())
    } else {
        if let Ok(has_fspath) = path.hasattr("__fspath__") {
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
}

#[pyfunction]
pub fn stat<'a>(py: Python<'a>, path: Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
    let path_str = path_to_string(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let _metadata = fs::metadata(&path_str).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        
        Python::with_gil(|py| {
            let os_module = py.import_bound("os")?;
            let result = os_module.call_method1("stat", (&path_str,))?;
            Ok(result.unbind())
        })
    })
}

#[pyfunction]
pub fn remove<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        fs::remove_file(&path).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
pub fn unlink<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
    remove(py, path)
}

#[pyfunction]
pub fn rename<'a>(py: Python<'a>, src: String, dst: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        fs::rename(&src, &dst).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
pub fn replace<'a>(py: Python<'a>, src: String, dst: String) -> PyResult<Bound<'a, PyAny>> {
    rename(py, src, dst)
}

#[pyfunction]
pub fn renames<'a>(py: Python<'a>, old: String, new: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        if let Some(parent) = std::path::Path::new(&new).parent() {
            fs::create_dir_all(parent).await.ok();
        }
        fs::rename(&old, &new).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        
        if let Some(parent) = std::path::Path::new(&old).parent() {
            fs::remove_dir(parent).await.ok();
        }
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
pub fn removedirs<'a>(py: Python<'a>, name: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut path = std::path::PathBuf::from(&name);
        loop {
            fs::remove_dir(&path).await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            
            if let Some(parent) = path.parent() {
                if parent.as_os_str().is_empty() {
                    break;
                }
                path = parent.to_path_buf();
            } else {
                break;
            }
        }
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[cfg(unix)]
#[pyfunction]
pub fn symlink<'a>(py: Python<'a>, src: String, dst: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::fs::symlink(&src, &dst).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[cfg(unix)]
#[pyfunction]
pub fn readlink<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let target = tokio::fs::read_link(&path).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Python::with_gil(|py| {
            Ok(PyString::new_bound(py, &target.to_string_lossy()).into_any().unbind())
        })
    })
}

#[cfg(unix)]
#[pyfunction]
pub fn link<'a>(py: Python<'a>, src: String, dst: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::fs::hard_link(&src, &dst).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
pub fn access<'a>(py: Python<'a>, path: Bound<'a, PyAny>, mode: i32) -> PyResult<Bound<'a, PyAny>> {
    let path_str = path_to_string(&path)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let metadata = fs::metadata(&path_str).await;
        
        if mode == 0 {
            let exists = metadata.is_ok();
            return Ok(Python::with_gil(|py| exists.into_py(py)));
        }
        
        let metadata = match metadata {
            Ok(meta) => meta,
            Err(_) => return Ok(Python::with_gil(|py| false.into_py(py))),
        };
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = metadata.permissions().mode();
            
            let accessible = match mode {
                1 => perms & 0o111 != 0,  // Execute permission
                2 => perms & 0o222 != 0,  // Write permission  
                4 => perms & 0o444 != 0,  // Read permission
                _ => false,               // Invalid mode
            };
            
            Ok(Python::with_gil(|py| accessible.into_py(py)))
        }
        
        #[cfg(not(unix))]
        {
            Ok(Python::with_gil(|py| true.into_py(py)))
        }
    })
}

#[pyclass]
pub struct AsyncDirEntry {
    name: String,
    path: PathBuf,
    is_dir: bool,
    is_file: bool,
}

#[pymethods]
impl AsyncDirEntry {
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }
    
    #[getter]
    fn path(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
    
    fn is_dir(&self) -> bool {
        self.is_dir
    }
    
    fn is_file(&self) -> bool {
        self.is_file
    }
}

#[pyfunction]
#[pyo3(signature = (path=None))]
pub fn scandir<'a>(py: Python<'a>, path: Option<String>) -> PyResult<Bound<'a, PyAny>> {
    let path_str = path.unwrap_or_else(|| ".".to_string());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let metadata = tokio::fs::metadata(&path_str).await
            .map_err(|e| {
                Python::with_gil(|py| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        pyo3::exceptions::PyFileNotFoundError::new_err(format!("[Errno 2] No such file or directory: '{}'", path_str))
                    } else {
                        os_err(py, e)
                    }
                })
            })?;
        
        if !metadata.is_dir() {
            return Err(Python::with_gil(|_py| {
                pyo3::exceptions::PyNotADirectoryError::new_err(format!("[Errno 20] Not a directory: '{}'", path_str))
            }));
        }
        
        let mut entries_list = Vec::new();
        let mut dir_entries = fs::read_dir(&path_str).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        
        while let Some(entry) = dir_entries.next_entry().await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))? {
            let metadata = entry.metadata().await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            
            let dir_entry = AsyncDirEntry {
                name: entry.file_name().to_string_lossy().to_string(),
                path: entry.path(),
                is_dir: metadata.is_dir(),
                is_file: metadata.is_file(),
            };
            
            
            Python::with_gil(|py| {
                entries_list.push(Py::new(py, dir_entry).unwrap());
            });
        }
        
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            for entry in entries_list {
                list.append(entry)?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
#[pyo3(signature = (path, _mode=None))]
pub fn mkdir<'a>(py: Python<'a>, path: String, _mode: Option<u32>) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        fs::create_dir(&path).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
#[pyo3(signature = (path, _mode=None, exist_ok=None))]
pub fn makedirs<'a>(py: Python<'a>, path: String, _mode: Option<u32>, exist_ok: Option<bool>) -> PyResult<Bound<'a, PyAny>> {
    let exist_ok = exist_ok.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        match fs::create_dir_all(&path).await {
            Ok(_) => Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py))),
            Err(e) if exist_ok && e.kind() == std::io::ErrorKind::AlreadyExists => Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py))),
            Err(e) => Err(Python::with_gil(|py| os_err(py, e)))
        }
    })
}

#[pyfunction]
pub fn rmdir<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        fs::remove_dir(&path).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Ok(Python::with_gil(|py| <() as pyo3::IntoPy<Py<PyAny>>>::into_py((), py)))
    })
}

#[pyfunction]
#[pyo3(signature = (path=None))]
pub fn listdir<'a>(py: Python<'a>, path: Option<String>) -> PyResult<Bound<'a, PyAny>> {
    let path_str = path.unwrap_or_else(|| ".".to_string());
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let metadata = tokio::fs::metadata(&path_str).await
            .map_err(|e| {
                Python::with_gil(|py| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        pyo3::exceptions::PyFileNotFoundError::new_err(format!("[Errno 2] No such file or directory: '{}'", path_str))
                    } else {
                        os_err(py, e)
                    }
                })
            })?;
        
        if !metadata.is_dir() {
            return Err(Python::with_gil(|_py| {
                pyo3::exceptions::PyNotADirectoryError::new_err(format!("[Errno 20] Not a directory: '{}'", path_str))
            }));
        }
        
        let mut entries = fs::read_dir(&path_str).await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))? {
            names.push(entry.file_name().to_string_lossy().to_string());
        }
        
        Python::with_gil(|py| {
            let list = PyList::empty_bound(py);
            for name in names {
                list.append(PyString::new_bound(py, &name))?;
            }
            Ok(list.into_any().unbind())
        })
    })
}

#[pyfunction]
pub fn getcwd<'a>(py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let cwd = std::env::current_dir()
            .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
        Python::with_gil(|py| {
            Ok(PyString::new_bound(py, &cwd.to_string_lossy()).into_any().unbind())
        })
    })
}

mod path {
    use super::*;
    
    #[pyfunction]
    pub fn exists<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let exists = fs::try_exists(&path).await.unwrap_or(false);
            Ok(Python::with_gil(|py| exists.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn isfile<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let is_file = fs::metadata(&path).await
                .map(|m| m.is_file())
                .unwrap_or(false);
            Ok(Python::with_gil(|py| is_file.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn isdir<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let is_dir = fs::metadata(&path).await
                .map(|m| m.is_dir())
                .unwrap_or(false);
            Ok(Python::with_gil(|py| is_dir.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn getsize<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let size = fs::metadata(&path).await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?
                .len();
            Ok(Python::with_gil(|py| size.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn abspath<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let absolute = std::fs::canonicalize(&path)
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            Python::with_gil(|py| {
            Ok(PyString::new_bound(py, &absolute.to_string_lossy()).into_any().unbind())
        })
        })
    }
    
    #[pyfunction]
    pub fn islink<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let is_link = fs::symlink_metadata(&path).await
                .map(|m| m.is_symlink())
                .unwrap_or(false);
            Ok(Python::with_gil(|py| is_link.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn getmtime<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = fs::metadata(&path).await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            let mtime = metadata.modified()
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| value_err(&e.to_string()))?
                .as_secs_f64();
            Ok(Python::with_gil(|py| mtime.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn getatime<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = fs::metadata(&path).await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            let atime = metadata.accessed()
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| value_err(&e.to_string()))?
                .as_secs_f64();
            Ok(Python::with_gil(|py| atime.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn getctime<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = fs::metadata(&path).await
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?;
            let ctime = metadata.created()
                .map_err(|e| Python::with_gil(|py| os_err(py, e)))?
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| value_err(&e.to_string()))?
                .as_secs_f64();
            Ok(Python::with_gil(|py| ctime.into_py(py)))
        })
    }
    
    #[pyfunction]
    pub fn ismount<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::with_gil(|py| {
                let os_path = py.import_bound("os.path")?;
                let result = os_path.call_method1("ismount", (&path,))?;
                Ok(result.unbind())
            })
        })
    }
    
    #[pyfunction]
    pub fn samefile<'a>(py: Python<'a>, path1: String, path2: String) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::with_gil(|py| {
                let os_path = py.import_bound("os.path")?;
                let result = os_path.call_method1("samefile", (&path1, &path2))?;
                Ok(result.unbind())
            })
        })
    }
    
    #[pyfunction]
    pub fn sameopenfile<'a>(py: Python<'a>, fd1: i32, fd2: i32) -> PyResult<Bound<'a, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Python::with_gil(|py| {
                let os_path = py.import_bound("os.path")?;
                let result = os_path.call_method1("sameopenfile", (fd1, fd2))?;
                Ok(result.unbind())
            })
        })
    }
}

#[cfg(unix)]
#[pyfunction]
pub fn statvfs<'a>(py: Python<'a>, path: String) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Python::with_gil(|py| {
            let os_module = py.import_bound("os")?;
            let result = os_module.call_method1("statvfs", (&path,))?;
            Ok(result.unbind())
        })
    })
}

#[cfg(unix)]
#[pyfunction]
#[pyo3(signature = (out_fd, in_fd, offset, count))]
pub fn sendfile<'a>(py: Python<'a>, out_fd: i32, in_fd: i32, offset: Option<i64>, count: i64) -> PyResult<Bound<'a, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Python::with_gil(|py| {
            let os_module = py.import_bound("os")?;
            let result = if let Some(off) = offset {
                os_module.call_method1("sendfile", (out_fd, in_fd, off, count))?
            } else {
                os_module.call_method1("sendfile", (out_fd, in_fd, py.None(), count))?
            };
            Ok(result.unbind())
        })
    })
}

pub fn register_os_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(stat, m)?)?;
    m.add_function(wrap_pyfunction!(remove, m)?)?;
    m.add_function(wrap_pyfunction!(unlink, m)?)?;
    m.add_function(wrap_pyfunction!(rename, m)?)?;
    m.add_function(wrap_pyfunction!(renames, m)?)?;
    m.add_function(wrap_pyfunction!(replace, m)?)?;
    m.add_function(wrap_pyfunction!(mkdir, m)?)?;
    m.add_function(wrap_pyfunction!(makedirs, m)?)?;
    m.add_function(wrap_pyfunction!(rmdir, m)?)?;
    m.add_function(wrap_pyfunction!(removedirs, m)?)?;
    m.add_function(wrap_pyfunction!(listdir, m)?)?;
    m.add_function(wrap_pyfunction!(scandir, m)?)?;
    m.add_function(wrap_pyfunction!(access, m)?)?;
    m.add_function(wrap_pyfunction!(getcwd, m)?)?;
    
    #[cfg(unix)]
    {
        m.add_function(wrap_pyfunction!(symlink, m)?)?;
        m.add_function(wrap_pyfunction!(readlink, m)?)?;
        m.add_function(wrap_pyfunction!(link, m)?)?;
        m.add_function(wrap_pyfunction!(statvfs, m)?)?;
        m.add_function(wrap_pyfunction!(sendfile, m)?)?;
    }
    
    let path_module = PyModule::new_bound(py, "path")?;
    path_module.add_function(wrap_pyfunction!(path::exists, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::isfile, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::isdir, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::getsize, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::abspath, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::islink, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::getmtime, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::getatime, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::getctime, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::ismount, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::samefile, m)?)?;
    path_module.add_function(wrap_pyfunction!(path::sameopenfile, m)?)?;
    m.add_submodule(&path_module)?;
    
    Ok(())
}
