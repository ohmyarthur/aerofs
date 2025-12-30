"""Type stubs for aerofs.tempfile module."""

from typing import Any, AsyncIterator, Optional, Union
from os import PathLike

class NamedTemporaryFile:
    """Async temporary file with a name on the filesystem."""
    
    name: str
    
    async def __aenter__(self) -> "NamedTemporaryFile": ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None: ...
    def __aiter__(self) -> AsyncIterator[Union[str, bytes]]: ...
    async def __anext__(self) -> Union[str, bytes]: ...
    
    async def read(self, size: Optional[int] = None) -> Union[str, bytes]: ...
    async def readinto(self, buffer: bytearray) -> int: ...
    async def write(self, data: Union[str, bytes]) -> int: ...
    async def flush(self) -> None: ...
    async def seek(self, offset: int, whence: int = 0) -> int: ...
    async def close(self) -> None: ...

class TemporaryDirectory:
    """Async temporary directory that is cleaned up on exit."""
    
    name: Optional[str]
    
    async def __aenter__(self) -> str: ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None: ...
    async def cleanup(self) -> None: ...

class SpooledTemporaryFile:
    """
    Async wrapper around Python's SpooledTemporaryFile.
    
    Note: This is actually a threadpool-wrapped synchronous SpooledTemporaryFile.
    """
    
    async def read(self, *args: Any, **kwargs: Any) -> Union[str, bytes]: ...
    async def write(self, *args: Any, **kwargs: Any) -> int: ...
    async def seek(self, *args: Any, **kwargs: Any) -> int: ...
    async def tell(self) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...
    
    async def __aenter__(self) -> "SpooledTemporaryFile": ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None: ...

# TemporaryFile is an alias for NamedTemporaryFile
TemporaryFile = NamedTemporaryFile

def named_temporary_file(
    mode: str = "w+b",
    buffering: int = -1,
    encoding: Optional[str] = None,
    newline: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
    delete: bool = True,
    *,
    delete_on_close: Optional[bool] = None,
) -> NamedTemporaryFile:
    """Create a named temporary file."""
    ...

def temporary_directory(
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
    dir: Optional[Union[str, PathLike[str]]] = None,
) -> TemporaryDirectory:
    """Create a temporary directory."""
    ...

def spooled_temporary_file(
    max_size: int = 0,
    mode: str = "w+b",
    buffering: int = -1,
    encoding: Optional[str] = None,
    newline: Optional[str] = None,
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[str] = None,
) -> SpooledTemporaryFile:
    """Create a spooled temporary file."""
    ...

__all__: list[str]
