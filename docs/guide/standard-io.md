# Standard I/O

`aerofs` provides non-blocking access to stdin, stdout, and stderr.

## Text Streams

Access standard text streams via `aerofs.stdin`, `aerofs.stdout`, and `aerofs.stderr`.

```python
import aerofs

# Write to stdout
await aerofs.stdout.write("Hello world\n")

# Read from stdin (iterating line by line)
async for line in aerofs.stdin:
    await aerofs.stdout.write(f"Eco: {line}")
```

## Binary Streams

For raw bytes, use the `_bytes` variants:

```python
# specific for binary data
data = await aerofs.stdin_bytes.read(1024)
await aerofs.stdout_bytes.write(b'\x00\x01\x02')
```
