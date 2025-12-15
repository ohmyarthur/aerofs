# Core API

The core module provides the essential interface for file I/O operations.

## `aerofs.open`

### `open`

```python
async def open(
    file: PathLike | str,
    mode: str = 'r',
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,
    opener: Callable | None = None
) -> AsyncFile
```

Open file and return a corresponding stream.

- **Parameters**:
  - `file`: Path to the file (string or PathLike).
  - `mode`: The mode in which the file is opened (default `'r'`). 
    - `'r'`: open for reading (default)
    - `'w'`: open for writing, truncating the file first
    - `'x'`: open for exclusive creation, failing if the file already exists
    - `'a'`: open for writing, appending to the end of file if it exists
    - `'b'`: binary mode
    - `'t'`: text mode (default)
    - `'+'`: open for updating (reading and writing)
  - `buffering`: The buffering policy.
  - `encoding`: Text encoding (e.g. `'utf-8'`).
  - `errors`: Error handling policy for encoding errors.
  - `newline`: Controls logical newline handling.

- **Returns**: `AsyncFile` context manager.

---

## `AsyncFile`

A file-like object returned by `open()`.

### `read`
```python
async def read(size: int | None = -1) -> str | bytes
```
Read and return at most `size` bytes/characters from the stream. If `size` is -1 or None, read until EOF.

### `read1`
```python
async def read1(size: int | None = -1) -> bytes
```
Read up to `size` bytes with at most one underlying `read()` call. (Binary mode only).

### `readline`
```python
async def readline(size: int | None = -1) -> str | bytes
```
Read a single line from the stream. If `size` is specified, reads at most that many bytes/characters.

### `readlines`
```python
async def readlines(hint: int | None = -1) -> list[str] | list[bytes]
```
Read and return a list of lines from the stream. `hint` can be used to control the number of lines read.

### `write`
```python
async def write(data: str | bytes) -> int
```
Write the string or bytes `data` to the file. Returns the number of bytes/characters written.

### `writelines`
```python
async def writelines(lines: Iterable[str] | Iterable[bytes]) -> None
```
Write a list of lines to the stream. Line separators are not added.

### `seek`
```python
async def seek(offset: int, whence: int = 0) -> int
```
Change the stream position.
- `whence` values:
  - `0`: Start of stream (default)
  - `1`: Current stream position
  - `2`: End of stream

### `tell`
```python
async def tell() -> int
```
Return the current stream position.

### `truncate`
```python
async def truncate(size: int | None = None) -> int
```
Resize the stream to the given `size` (or the current position if `size` is None).

### `flush`
```python
async def flush() -> None
```
Flush the write buffer of the stream if applicable.

### `close`
```python
async def close() -> None
```
Flush and close the stream.

### Properties
- `closed`: `bool` — True if the file is closed.
- `mode`: `str` — The I/O mode for the file.
- `name`: `str` — The name of the file.
