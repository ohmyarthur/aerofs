# Getting Started

**aerofs** provides a high-performance, asynchronous interface for file system operations in Python.

## Installation

Install via pip (or uv):

```shell
pip install aerofs
# or
uv pip install aerofs
```

> [!NOTE] Support
> Currently supports **Linux** and **macOS** (Intel & Apple Silicon). Windows support is planned.

## Why aerofs?

In standard Python `asyncio`, file operations (like `open().read()`) are blocking. This means they pause the entire event loop, potentially stalling other concurrent tasks (like network requests).

`aerofs` uses a background thread pool managed by Rust's `tokio`, allowing file operations to run concurrently without blocking the Python main thread.

## Basic Usage

The API is designed to mirror Python's built-in functions.

### Reading a File

```python
import aerofs
import asyncio

async def main():
    async with aerofs.open('example.txt', 'r') as f:
        content = await f.read()
        print(content)

asyncio.run(main())
```

### Writing a File

```python
async with aerofs.open('output.txt', 'w') as f:
    await f.write("Writing asynchronously is fast!")
```
