"""
aerofs: High-Performance Asynchronous File I/O for Python, powered by Rust.

Asynchronous
Efficient
Rust
Operation
File
System
"""

from ._aerofs import (
    open,
    stdin,
    stdout,
    stderr,
    stdin_bytes,
    stdout_bytes,
    stderr_bytes,
)

from . import os
from . import tempfile
from . import threadpool

__version__ = "4.0.7"
__author__ = "ohmyarthur"
__all__ = [
    "open",
    "stdin",
    "stdout",
    "stderr",
    "stdin_bytes",
    "stdout_bytes",
    "stderr_bytes",
    "os",
    "tempfile",
    "threadpool",
]
