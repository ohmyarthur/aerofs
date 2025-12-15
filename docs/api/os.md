# OS Operations

The `aerofs.os` module mirrors the Python standard `os` module with asynchronous counterparts.

## File Operations

### `rename`
```python
async def rename(src: str, dst: str) -> None
```
Rename the file or directory `src` to `dst`.

### `renames`
```python
async def renames(old: str, new: str) -> None
```
Recursive directory or file renaming function.

### `replace`
```python
async def replace(src: str, dst: str) -> None
```
Rename the file or directory `src` to `dst`, overwriting `dst` if it exists.

### `remove` / `unlink`
```python
async def remove(path: str) -> None
```
Remove (delete) the file `path`.

### `mkdir`
```python
async def mkdir(path: str, mode: int = 0o777) -> None
```
Create a directory named `path` with numeric mode `mode`.

### `makedirs`
```python
async def makedirs(path: str, mode: int = 0o777, exist_ok: bool = False) -> None
```
Recursive directory creation function. If `exist_ok` is True, behaves like `mkdir -p`.

### `rmdir`
```python
async def rmdir(path: str) -> None
```
Remove (delete) the directory `path`.

### `removedirs`
```python
async def removedirs(name: str) -> None
```
Remove directories recursively.

## Filesystem Stats & Queries

### `stat`
```python
async def stat(path: str | PathLike) -> os.stat_result
```
Get the status of a file or a file descriptor.

### `access`
```python
async def access(path: str | PathLike, mode: int) -> bool
```
Use the real uid/gid to test for access to `path`.

### `listdir`
```python
async def listdir(path: str | None = None) -> list[str]
```
Return a list containing the names of the entries in the directory given by `path`.

### `scandir`
```python
async def scandir(path: str | None = None) -> list[AsyncDirEntry]
```
Return an iterator of `AsyncDirEntry` objects corresponding to the entries in the directory given by `path`.

### `getcwd`
```python
async def getcwd() -> str
```
Return a string representing the current working directory.

## Unix Specific

### `symlink`
```python
async def symlink(src: str, dst: str) -> None
```
Create a symbolic link pointing to `src` named `dst`.

### `readlink`
```python
async def readlink(path: str) -> str
```
Return a string representing the path to which the symbolic link points.

### `link`
```python
async def link(src: str, dst: str) -> None
```
Create a hard link pointing to `src` named `dst`.

### `statvfs`
```python
async def statvfs(path: str) -> os.statvfs_result
```
Perform a `statvfs()` system call on the given path.

### `sendfile`
```python
async def sendfile(out_fd: int, in_fd: int, offset: int | None, count: int) -> int
```
Copy `count` bytes from file descriptor `in_fd` to file descriptor `out_fd` starting at `offset`.

---

# Path Operations (`aerofs.os.path`)

### `exists`
```python
async def exists(path: str) -> bool
```
Return True if `path` refers to an existing path or an open file descriptor.

### `isfile`
```python
async def isfile(path: str) -> bool
```
Return True if `path` is an existing regular file.

### `isdir`
```python
async def isdir(path: str) -> bool
```
Return True if `path` is an existing directory.

### `getsize`
```python
async def getsize(path: str) -> int
```
Return the size of a file, reported by `os.stat()`.

### `abspath`
```python
async def abspath(path: str) -> str
```
Return an absolute path.

### `islink`
```python
async def islink(path: str) -> bool
```
Return True if `path` refers to an existing directory entry that is a symbolic link.

### `getmtime`, `getatime`, `getctime`
```python
async def getmtime(path: str) -> float
```
Return the time of last modification/access/metadata-change.

### `ismount`
```python
async def ismount(path: str) -> bool
```
Return True if pathname `path` is a mount point.

### `samefile`
```python
async def samefile(path1: str, path2: str) -> bool
```
Return True if both pathname arguments refer to the same file or directory.

### `sameopenfile`
```python
async def sameopenfile(fp1: int, fp2: int) -> bool
```
Return True if both file descriptors refer to the same file.
