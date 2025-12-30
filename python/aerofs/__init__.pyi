"""
aerofs: High-Performance Asynchronous File I/O for Python, powered by Rust.
"""

from typing import (
    Any,
    AnyStr,
    AsyncIterator,
    Literal,
    Optional,
    Union,
    overload,
)
from os import PathLike

__version__: str
__author__: str
__all__: list[str]

class AsyncFile:
    """Asynchronous file object returned by aerofs.open()."""
    
    closed: bool
    name: str
    mode: str
    
    async def __aenter__(self) -> "AsyncFile": ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None: ...
    def __aiter__(self) -> AsyncIterator[Union[str, bytes]]: ...
    async def __anext__(self) -> Union[str, bytes]: ...
    
    async def read(self, size: Optional[int] = None) -> Union[str, bytes]: ...
    async def read1(self, size: Optional[int] = None) -> Union[str, bytes]: ...
    async def readall(self) -> Union[str, bytes]: ...
    async def readline(self, size: Optional[int] = None) -> Union[str, bytes]: ...
    async def readlines(self, hint: Optional[int] = None) -> list[Union[str, bytes]]: ...
    async def readinto(self, buffer: bytearray) -> int: ...
    
    async def write(self, data: Union[str, bytes]) -> int: ...
    async def writelines(self, lines: list[Union[str, bytes]]) -> None: ...
    
    async def seek(self, offset: int, whence: int = 0) -> int: ...
    async def tell(self) -> int: ...
    async def truncate(self, size: Optional[int] = None) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...
    
    async def peek(self, size: Optional[int] = None) -> Union[str, bytes]: ...
    def fileno(self) -> int: ...
    def detach(self) -> Any: ...
    
    def readable(self) -> bool: ...
    def writable(self) -> bool: ...
    def seekable(self) -> bool: ...
    def isatty(self) -> bool: ...

class AsyncStdin:
    """Asynchronous stdin reader."""
    async def read(self) -> None:
        """Not supported in async context. Raises OSError."""
        ...
    async def readline(self) -> Union[str, bytes]: ...

class AsyncStdout:
    """Asynchronous stdout writer."""
    async def write(self, data: Union[str, bytes]) -> int: ...
    async def flush(self) -> None: ...

class AsyncStderr:
    """Asynchronous stderr writer."""
    async def write(self, data: Union[str, bytes]) -> int: ...
    async def flush(self) -> None: ...

stdin: AsyncStdin
stdout: AsyncStdout
stderr: AsyncStderr
stdin_bytes: AsyncStdin
stdout_bytes: AsyncStdout
stderr_bytes: AsyncStderr

@overload
def open(
    file: Union[str, PathLike[str]],
    mode: Literal["r", "r+", "w", "w+", "a", "a+", "x", "x+"] = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Any] = None,
    loop: Optional[Any] = None,
    executor: Optional[Any] = None,
) -> AsyncFile: ...

@overload
def open(
    file: Union[str, PathLike[str]],
    mode: Literal["rb", "rb+", "r+b", "wb", "wb+", "w+b", "ab", "ab+", "a+b", "xb", "xb+", "x+b"],
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Any] = None,
    loop: Optional[Any] = None,
    executor: Optional[Any] = None,
) -> AsyncFile: ...

def open(
    file: Union[str, PathLike[str]],
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Any] = None,
    loop: Optional[Any] = None,
    executor: Optional[Any] = None,
) -> AsyncFile:
    """
    Open a file asynchronously.
    
    This is a drop-in replacement for the built-in open() function,
    returning an AsyncFile object that supports async context managers
    and async iteration.
    """
    ...

from . import os as os
from . import tempfile as tempfile
from . import threadpool as threadpool
