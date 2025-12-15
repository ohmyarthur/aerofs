# Tempfile

The `aerofs.tempfile` module provides async versions of Python's `tempfile` resources.

## `NamedTemporaryFile`

```python
def named_temporary_file(
    mode: str = 'w+b',
    buffering: int = -1,
    encoding: str | None = None,
    newline: str | None = None,
    suffix: str | None = None,
    prefix: str | None = None,
    dir: str | None = None,
    delete: bool = True,
    delete_on_close: bool = True
) -> AsyncTemporaryFile
```

Create and return a temporary file with a visible name in the file system.

- **Parameters**:
  - `mode`: Mode to open file (default `'w+b'`).
  - `delete` / `delete_on_close`: If true, file is deleted when closed.
  - Other parameters match `aerofs.open`.

### `AsyncTemporaryFile` Methods
- `name`: Returns the path to the tempfile.
- All standard `AsyncFile` methods (`read`, `write`, `seek`, `close`, etc.).

## `TemporaryDirectory`

```python
def temporary_directory(
    suffix: str | None = None,
    prefix: str | None = None,
    dir: str | None = None
) -> AsyncTemporaryDirectory
```

Create and return a temporary directory.

### `AsyncTemporaryDirectory`
- **Context Manager**:
  ```python
  async with aerofs.tempfile.TemporaryDirectory() as temp_dir:
      print(temp_dir) # path to directory
  ```
- **Methods**:
  - `cleanup()`: Explicitly remove the directory and contents.
  - `name`: Path to the directory.

## `SpooledTemporaryFile`

```python
def spooled_temporary_file(
    max_size: int = 0,
    mode: str = 'w+b',
    buffering: int = -1,
    encoding: str | None = None,
    newline: str | None = None,
    suffix: str | None = None,
    prefix: str | None = None,
    dir: str | None = None
) -> ThreadPoolWrapper
```

Creates a `tempfile.SpooledTemporaryFile` (wrapped in a thread pool for async access) that buffers data in memory until it exceeds `max_size`.
