from ._aerofs.os import (
    stat,
    remove,
    unlink,
    rename,
    replace,
    mkdir,
    makedirs,
    rmdir,
    listdir,
    getcwd,
    access,
    scandir as _scandir,
    renames,
    removedirs,
    path,
)

async def scandir(path=None):
    entries = await _scandir(path)
    return iter(entries)

try:
    from ._aerofs.os import (
        link,
        symlink,
        readlink,
        statvfs,
        sendfile,
    )
except ImportError:
    pass

__all__ = [
    "stat",
    "remove",
    "unlink",
    "rename",
    "replace",
    "mkdir",
    "makedirs",
    "rmdir",
    "removedirs",
    "listdir",
    "getcwd",
    "access",
    "scandir",
    "renames",
    "path",
    "link",
    "symlink",
    "readlink",
    "statvfs",
    "sendfile",
]
