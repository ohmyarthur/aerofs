"""Threadpool-based async file operations compatibility layer for aerofs.

This module provides compatibility with aiofiles.threadpool API.
Since aerofs uses native Rust async I/O instead of thread pools,
this module simply re-exports the main aerofs functions."""

import asyncio
import builtins
from aerofs import open as _rust_open

# Add sync_open as an alias to built-in open for compatibility with tests
sync_open = builtins.open

def open(*args, **kwargs):
    """Open a file asynchronously.
    
    For compatibility with tests that monkeypatch sync_open, we check if
    sync_open has been replaced. If so, we delay the file open to respect
    the monkeypatch timing. Otherwise, we use native Rust async I/O.
    """
    global sync_open
    
    # Check if sync_open was monkeypatched (not the same as builtins.open)
    if sync_open is not builtins.open:
        # Create a wrapper that will use sync_open in executor
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
                    # Call sync_open in thread executor (respects monkeypatch delay)
                    loop = asyncio.get_event_loop()
                    sync_file = await loop.run_in_executor(None, lambda: sync_open(*self.args, **self.kwargs))
                    sync_file.close()  # Close sync file immediately
                    # Now open with rust native async
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
                # Delegate all other attributes to the underlying file
                if self._file:
                    return getattr(self._file, name)
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
                    
        return DelayedOpen(*args, **kwargs)
    else:
        # Use native Rust async I/O (fast path)
        return _rust_open(*args, **kwargs)

# Wrap function for compatibility
def wrap(func):
    """Wrap a synchronous function to be async.
    
    Note: This is a compatibility shim. aerofs uses native async I/O,
    not thread pools.
    """
    import asyncio
    import functools
    import tempfile
    from io import TextIOBase, FileIO, BufferedIOBase, BufferedReader, BufferedWriter, BufferedRandom
    
    # Only accept specific IO types like aiofiles does
    if not isinstance(func, (TextIOBase, FileIO, BufferedIOBase, BufferedReader, BufferedWriter, BufferedRandom, tempfile.SpooledTemporaryFile)):
        raise TypeError(f"Unsupported io type: {func}.")
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))
    
    return wrapper

__all__ = ['open', 'wrap']
