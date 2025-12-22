# Standard I/O

`aerofs` provides non-blocking access to stdin, stdout, and stderr for use in async applications.

## Text Streams

Access standard text streams via `aerofs.stdin`, `aerofs.stdout`, and `aerofs.stderr`.

### Writing to stdout

```python
import aerofs

async def greet():
    await aerofs.stdout.write("Hello, async world!\n")
    await aerofs.stdout.flush()
```

### Reading from stdin

> [!NOTE] Interactive Input
> The `readline()` method reads a single line from stdin asynchronously.

```python
import aerofs

async def get_input():
    await aerofs.stdout.write("Enter your name: ")
    await aerofs.stdout.flush()
    
    name = await aerofs.stdin.readline()
    await aerofs.stdout.write(f"Hello, {name}")
```

### Writing to stderr

```python
import aerofs

async def log_error():
    await aerofs.stderr.write("Error: Something went wrong\n")
    await aerofs.stderr.flush()
```

## Binary Streams

For raw byte-level I/O, use the `_bytes` variants:

- `aerofs.stdin_bytes`
- `aerofs.stdout_bytes`  
- `aerofs.stderr_bytes`

```python
import aerofs

async def binary_io():
    # Read binary data from stdin
    data = await aerofs.stdin_bytes.readline()
    
    # Write binary data to stdout
    await aerofs.stdout_bytes.write(b'\x00\x01\x02\x03')
    await aerofs.stdout_bytes.flush()
```

## Available Methods

### AsyncStdin
| Method | Description |
|--------|-------------|
| `readline()` | Read a line from stdin |

### AsyncStdout / AsyncStderr
| Method | Description |
|--------|-------------|
| `write(data)` | Write string or bytes |
| `flush()` | Flush the buffer |
