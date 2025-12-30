from ._aerofs import open as _rust_open
import asyncio


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


class AsyncWrapper:
    """Wraps a synchronous file-like object for async use."""
    
    def __init__(self, obj):
        self._obj = obj
        self._loop = None
    
    def _get_loop(self):
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop
    
    async def read(self, *args, **kwargs):
        return await self._get_loop().run_in_executor(
            None, lambda: self._obj.read(*args, **kwargs)
        )
    
    async def write(self, *args, **kwargs):
        return await self._get_loop().run_in_executor(
            None, lambda: self._obj.write(*args, **kwargs)
        )
    
    async def seek(self, *args, **kwargs):
        return await self._get_loop().run_in_executor(
            None, lambda: self._obj.seek(*args, **kwargs)
        )
    
    async def tell(self):
        return await self._get_loop().run_in_executor(
            None, self._obj.tell
        )
    
    async def flush(self):
        return await self._get_loop().run_in_executor(
            None, self._obj.flush
        )
    
    async def close(self):
        return await self._get_loop().run_in_executor(
            None, self._obj.close
        )
    
    @property
    def name(self):
        return getattr(self._obj, 'name', None)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False


def wrap(entity):
    """Wrap a synchronous file-like object for async use."""
    return AsyncWrapper(entity)


__all__ = [
    "open",
    "wrap",
]

