# aerofs

<div align="center">
  <p><strong>High-Performance Asynchronous File I/O for Python</strong></p>
  <p>Powered by Rust, Tokio, and PyO3.</p>

  <p>
    <a href="https://pypi.org/project/aerofs/"><img src="https://img.shields.io/pypi/v/aerofs.svg" alt="PyPI"></a>
    <a href="https://github.com/ohmyarthur/aerofs/actions"><img src="https://github.com/ohmyarthur/aerofs/workflows/Release/badge.svg" alt="Release"></a>
    <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  </p>
</div>

---

**aerofs** brings the power of Rust's asynchronous I/O to Python. By leveraging `tokio` and `PyO3`, it provides a non-blocking, thread-safe, and highly performant filesystem interface for `asyncio` applications.

> **Full documentation at the [Documentation Site](https://ohmyarthur.github.io/aerofs/).**

## Installation

```shell
pip install aerofs
```

## Quick Start
```python
import aerofs
import asyncio

async def main():
    async with aerofs.open('hello.txt', 'w') as f:
        await f.write('Hello via Rust!')

    async with aerofs.open('hello.txt', 'r') as f:
        print(await f.read())

if __name__ == "__main__":
    asyncio.run(main())
```

## Documentation

Guides and API references are available in the `docs/` directory or online:

- [Getting Started](https://ohmyarthur.github.io/aerofs/guide/getting-started.html)
- [File Operations](https://ohmyarthur.github.io/aerofs/guide/file-operations.html)

## License

Apache 2.0 License.
