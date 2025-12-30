"""Type stubs for aerofs.threadpool module."""

from typing import Any, Optional, Union

class AsyncFileContextManager:
    """Context manager wrapper for async file operations."""
    
    def __await__(self) -> Any: ...
    async def __aenter__(self) -> Any: ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool: ...

class AsyncWrapper:
    """
    Wraps a synchronous file-like object for async use.
    
    Methods are executed in a thread pool executor to avoid blocking
    the event loop.
    """
    
    @property
    def name(self) -> Optional[str]: ...
    
    async def read(self, *args: Any, **kwargs: Any) -> Union[str, bytes]: ...
    async def write(self, *args: Any, **kwargs: Any) -> int: ...
    async def seek(self, *args: Any, **kwargs: Any) -> int: ...
    async def tell(self) -> int: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...
    
    async def __aenter__(self) -> "AsyncWrapper": ...
    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool: ...

def open(
    file: Any,
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    closefd: bool = True,
    opener: Optional[Any] = None,
    loop: Optional[Any] = None,
    executor: Optional[Any] = None,
) -> AsyncFileContextManager:
    """
    Open a file asynchronously using the threadpool wrapper.
    
    Returns an AsyncFileContextManager that yields an AsyncFile when used
    as an async context manager.
    """
    ...

def wrap(entity: Any) -> AsyncWrapper:
    """
    Wrap a synchronous file-like object for async use.
    
    Args:
        entity: Any file-like object with read/write/seek/tell/flush/close methods.
    
    Returns:
        AsyncWrapper that provides async versions of the file methods.
    
    Raises:
        TypeError: If entity is not a valid file-like object.
    """
    ...

__all__: list[str]
