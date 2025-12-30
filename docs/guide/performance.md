# Performance Tips

Optimize your AeroFS usage with these best practices.

## Binary Mode

Use binary mode (`'rb'`/`'wb'`) when possible to avoid UTF-8 encoding overhead:

```python
# Faster for raw data
async with aerofs.open('data.bin', 'rb') as f:
    content = await f.read()
```

## Batch Operations

Read or write larger chunks at once rather than small pieces:

```python
# Good: Single large write
async with aerofs.open('output.txt', 'w') as f:
    await f.write(large_text)

# Avoid: Many small writes
async with aerofs.open('output.txt', 'w') as f:
    for line in lines:
        await f.write(line)  # Less efficient
```

## Use writelines for Multiple Lines

```python
async with aerofs.open('output.txt', 'w') as f:
    await f.writelines(lines)  # Single operation
```

## Context Managers

Always use `async with` to ensure proper cleanup:

```python
async with aerofs.open('file.txt', 'r') as f:
    content = await f.read()
# File is automatically closed and flushed
```

## Concurrent Operations

Leverage asyncio for concurrent file operations:

```python
import asyncio

async def read_files(paths):
    async def read_one(path):
        async with aerofs.open(path, 'r') as f:
            return await f.read()
    
    return await asyncio.gather(*[read_one(p) for p in paths])
```

## Memory Considerations

For very large files, use chunked reading:

```python
async with aerofs.open('large.bin', 'rb') as f:
    while chunk := await f.read(1024 * 1024):  # 1MB chunks
        process(chunk)
```
