
from ._aerofs.tempfile import (
    named_temporary_file as NamedTemporaryFile,
    temporary_directory as TemporaryDirectory,
    spooled_temporary_file as SpooledTemporaryFile,
)

TemporaryFile = NamedTemporaryFile

__all__ = [
    "NamedTemporaryFile",
    "TemporaryFile",
    "TemporaryDirectory",
    "SpooledTemporaryFile",
]
