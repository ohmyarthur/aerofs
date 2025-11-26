from ._aerofs import open as _rust_open


class AsyncFileContextManager:
    def __init__(self, coro):
        self._coro = coro
        self._file = None
    
    def __await__(self):
        return self._coro.__await__()
    
    async def __aenter__(self):
        self._file = await self._coro
        return await self._file.__aenter__()
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._file is not None:
            return await self._file.__aexit__(exc_type, exc_val, exc_tb)
        return False


def open(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None,
         closefd=True, opener=None, loop=None, executor=None):
    coro = _rust_open(
        file, mode, buffering, encoding, errors, newline,
        closefd, opener, loop, executor
    )
    return AsyncFileContextManager(coro)


def wrap(entity):
    raise TypeError(f"Cannot wrap {type(entity).__name__}")


__all__ = [
    "open",
    "wrap",
]
