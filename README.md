# aerofs: High-Performance Asynchronous File I/O for Python

[![PyPI](https://img.shields.io/pypi/v/aerofs.svg)](https://pypi.org/project/aerofs/)
[![Release](https://github.com/ohmyarthur/aerofs/workflows/Release/badge.svg)](https://github.com/ohmyarthur/aerofs/actions)
[![Python Versions](https://img.shields.io/pypi/pyversions/aerofs.svg)](https://pypi.org/project/aerofs/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**aerofs** is an Apache 2.0 licensed library for handling local disk files in asyncio applications, written in Rust and powered by PyO3 and Tokio for maximum performance.

Ordinary local file I/O is blocking and cannot easily be made asynchronous. This means doing file I/O may interfere with asyncio applications, which shouldn't block the executing thread. **aerofs** solves this by providing asynchronous file operations with superior performance compared to pure Python alternatives like aiofiles.

```python
import aerofs

async with aerofs.open('filename', mode='r') as f:
    contents = await f.read()
print(contents)
```

Asynchronous iteration is also supported:

```python
async with aerofs.open('filename') as f:
    async for line in f:
        print(line)
```

Asynchronous interface to tempfile module:

```python
async with aerofs.tempfile.TemporaryFile('wb') as f:
    await f.write(b'Hello, World!')
```


## Installation

To install aerofs, simply:

```shell
uv pip install aerofs
```

> **Note:** Official wheels available for **Linux** and **macOS (Intel & Apple Silicon / universal2)**. Windows not yet supported.

## Usage

### Basic File Operations

Files are opened using the `aerofs.open()` coroutine, which mirrors Python's builtin `open()`:

```python
import aerofs

async def read_file():
    async with aerofs.open('example.txt', 'r') as f:
        content = await f.read()
        return content

async def write_file():
    async with aerofs.open('example.txt', 'w') as f:
        await f.write('Hello, World!')
```

The following methods are async and available on file objects:

- `close`
- `flush`
- `isatty`
- `read`
- `readall`
- `read1`
- `readinto`
- `readline`
- `readlines`
- `seek`
- `seekable`
- `tell`
- `truncate`
- `writable`
- `write`
- `writelines`

### Standard I/O

Async access to standard streams:

```python
import aerofs

async def read_stdin():
    async for line in aerofs.stdin:
        print(f"You typed: {line}")

async def write_stdout():
    await aerofs.stdout.write("Hello from async stdout!\n")
    await aerofs.stdout.flush()
```

Available streams:
- `aerofs.stdin`, `aerofs.stdout`, `aerofs.stderr`
- `aerofs.stdin_bytes`, `aerofs.stdout_bytes`, `aerofs.stderr_bytes`

### OS Operations

The `aerofs.os` module contains async versions of useful `os` functions:

```python
import aerofs.os

async def os_operations():
    # Check if file exists
    exists = await aerofs.os.path.exists('myfile.txt')
    
    # List directory
    files = await aerofs.os.listdir('.')
    
    # Get file stats
    stats = await aerofs.os.stat('myfile.txt')
    
    # File operations
    await aerofs.os.rename('old.txt', 'new.txt')
    await aerofs.os.remove('unwanted.txt')
    await aerofs.os.mkdir('newdir')
```

Available operations:
- `stat`, `statvfs`, `sendfile`
- `rename`, `renames`, `replace`, `remove`, `unlink`
- `mkdir`, `makedirs`, `rmdir`, `removedirs`
- `link`, `symlink`, `readlink`
- `listdir`, `scandir`, `access`, `getcwd`
- `path.abspath`, `path.exists`, `path.isfile`, `path.isdir`
- `path.islink`, `path.ismount`, `path.getsize`
- `path.getatime`, `path.getctime`, `path.samefile`, `path.sameopenfile`

### Tempfile

**aerofs.tempfile** implements async interfaces for temporary files:

```python
import aerofs.tempfile
import os

async def use_tempfile():
    # Named temporary file
    async with aerofs.tempfile.NamedTemporaryFile('wb+') as f:
        await f.write(b'Line1\nLine2')
        await f.seek(0)
        async for line in f:
            print(line)
    
    # Temporary directory
    async with aerofs.tempfile.TemporaryDirectory() as d:
        filename = os.path.join(d, "file.ext")
        async with aerofs.open(filename, 'w') as f:
            await f.write("temporary data")
```

Available interfaces:
- `TemporaryFile`
- `NamedTemporaryFile`
- `SpooledTemporaryFile`
- `TemporaryDirectory`

## Requirements

- Python 3.9 or higher
- Linux or macOS (Intel/Apple Silicon). Windows not yet supported.

## Contributing

Contributions are very welcome! Please feel free to submit a Pull Request.

### Development Setup

1. Clone the repository:
```shell
git clone https://github.com/ohmyarthur/aerofs.git
cd aerofs
```

2. Install development dependencies:
```shell
uv pip install -e ".[dev]"
```

3. Run tests:
```shell
pytest
```

### Building from Source

```shell
# Install maturin
uv pip install maturin

# Build the package
maturin develop

# Or build a wheel
maturin build --release
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Inspired by [aiofiles](https://github.com/Tinche/aiofiles), but built from the ground up with Rust .
