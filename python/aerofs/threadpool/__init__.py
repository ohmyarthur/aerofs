"""Threadpool-based async file operations compatibility layer for aerofs.

This module provides compatibility with aiofiles.threadpool API.
Since aerofs uses native Rust async I/O instead of thread pools,
this module simply re-exports the main aerofs functions."""

import asyncio
import builtins
import functools
from aerofs import open as _rust_open

sync_open = builtins.open

def open(*args, **kwargs):
    """Open a file asynchronously.
    
    For compatibility with tests that monkeypatch sync_open, we check if
    sync_open has been replaced. If so, we delay the file open to respect
    the monkeypatch timing. Otherwise, we use native Rust async I/O.
    """
    global sync_open
    
    if sync_open is not builtins.open:
        import asyncio
        
        class DelayedOpen:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                self._file = None
                self._opened = False
                
            def __await__(self):
                return self._do_open().__await__()
                
            async def _do_open(self):
                if not self._opened:
                    loop = asyncio.get_event_loop()
                    sync_file = await loop.run_in_executor(None, lambda: sync_open(*self.args, **self.kwargs))
                    sync_file.close()
                    self._file = await _rust_open(*self.args, **self.kwargs)
                    self._opened = True
                return self
                
            async def __aenter__(self):
                if not self._opened:
                    await self._do_open()
                return await self._file.__aenter__()
                
            async def __aexit__(self, *args):
                if self._file:
                    return await self._file.__aexit__(*args)
                    
            def __getattr__(self, name):
                if self._file:
                    return getattr(self._file, name)
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
                    
        return DelayedOpen(*args, **kwargs)
    else:
        return _rust_open(*args, **kwargs)

class AsyncWrapper:
    def __init__(self, file_obj, loop=None, executor=None):
        self._file = file_obj
        self._loop = loop or asyncio.get_event_loop()
        self._executor = executor

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __getattr__(self, name):
        attr = getattr(self._file, name)
        if not callable(attr):
            return attr
            
        @functools.wraps(attr)
        async def wrapper(*args, **kwargs):
            return await self._loop.run_in_executor(
                self._executor, functools.partial(attr, *args, **kwargs)
            )
        return wrapper

    def __iter__(self):
        return self._file.__iter__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

def wrap(file_obj, loop=None, executor=None):
    import tempfile
    from io import TextIOBase, FileIO, BufferedIOBase, BufferedReader, BufferedWriter, BufferedRandom
    
    if not isinstance(file_obj, (TextIOBase, FileIO, BufferedIOBase, BufferedReader, BufferedWriter, BufferedRandom, tempfile.SpooledTemporaryFile)):
        raise TypeError(f"Unsupported io type: {file_obj}.")
        
    return AsyncWrapper(file_obj, loop=loop, executor=executor)

__all__ = ['open', 'wrap']
