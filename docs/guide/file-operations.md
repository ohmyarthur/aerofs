# File Operations

`aerofs.open` is the primary entry point, functioning exactly like Python's `open()` but returning an awaitable context manager.

## Opening Files

```python
async with aerofs.open('data.csv', 'r') as f:
    ...
```

The supported modes are standard: `'r'`, `'w'`, `'a'`, `'rb'`, `'wb'`, `'ab'`, `'r+'`, `'w+'`, etc.

## Reading and Writing

### `read(size=-1)`
Reads *size* bytes/characters. If size is -1, reads the entire file.

```python
content = await f.read(1024)
```

### `readline()`
Reads a single line.

```python
line = await f.readline()
```

### `readlines()`
Reads all lines into a list.

```python
lines = await f.readlines()
```

### `write(data)`
Writes string or bytes to the file.

```python
await f.write("New data")
```

### Iteration
You can iterate over the file line-by-line asynchronously:

```python
async with aerofs.open('log.txt') as f:
    async for line in f:
        if "ERROR" in line:
            print(f"Found error: {line}")
```

## Seeking and Truncating

- **`seek(offset, whence=0)`**: Change stream position.
- **`tell()`**: Get current position.
- **`truncate(size=None)`**: Resize the file stream.

```python
await f.seek(0)
await f.truncate()
```

## Flushing and Syncing
Files are automatically flushed on close, but you can manually flush:

```python
await f.flush()
```
