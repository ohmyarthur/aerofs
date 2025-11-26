import asyncio
import tempfile
from ._aerofs.tempfile import (
    named_temporary_file as NamedTemporaryFile,
    temporary_directory as TemporaryDirectory,
)

class AsyncSpooledTemporaryFile:
    def __init__(self, max_size=0, mode="w+b", buffering=-1, 
                 encoding=None, newline=None, suffix=None, prefix=None, dir=None):
        self._max_size = max_size
        self._mode = mode
        self._buffering = buffering
        self._encoding = encoding
        self._newline = newline
        self._suffix = suffix
        self._prefix = prefix
        self._dir = dir
        self._file = None
    
    async def __aenter__(self):
        self._file = tempfile.SpooledTemporaryFile(
            max_size=self._max_size,
            mode=self._mode,
            buffering=self._buffering,
            encoding=self._encoding,
            newline=self._newline,
            suffix=self._suffix,
            prefix=self._prefix,
            dir=self._dir
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            await asyncio.to_thread(self._file.close)
    
    async def read(self, size=-1):
        return await asyncio.to_thread(self._file.read, size)
    
    async def write(self, data):
        return await asyncio.to_thread(self._file.write, data)
    
    async def flush(self):
        return await asyncio.to_thread(self._file.flush)
    
    async def seek(self, offset, whence=0):
        return await asyncio.to_thread(self._file.seek, offset, whence)
    
    async def tell(self):
        return await asyncio.to_thread(self._file.tell)
    
    @property
    def _file_attr(self):
        return self._file
    
    @property
    def name(self):
        return self._file.name if self._file else None
    
    @property
    def mode(self):
        return self._file.mode if self._file else self._mode
    
    @property
    def newlines(self):
        return getattr(self._file, 'newlines', None) if self._file else None

def SpooledTemporaryFile(*args, **kwargs):
    return AsyncSpooledTemporaryFile(*args, **kwargs)

TemporaryFile = NamedTemporaryFile

__all__ = [
    "NamedTemporaryFile",
    "TemporaryFile",
    "TemporaryDirectory",
    "SpooledTemporaryFile",
]
