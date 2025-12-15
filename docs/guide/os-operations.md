# OS Operations

The `aerofs.os` module provides async wrappers for common `os` and `os.path` functions.

## Filesystem Manipulation

```python
import aerofs.os

# Rename
await aerofs.os.rename('old_name.txt', 'new_name.txt')

# Remove (Unlink)
await aerofs.os.remove('file_to_delete.txt')

# Make Directory
await aerofs.os.mkdir('new_folder')
await aerofs.os.makedirs('nested/folder/path')

# Remove Directory
await aerofs.os.rmdir('empty_folder')
await aerofs.os.removedirs('nested/folder/path')
```

## Path Operations

`aerofs.os.path` mirrors `os.path`.

```python
# Check existence
if await aerofs.os.path.exists('config.json'):
    print("Config found")

# Check type
await aerofs.os.path.isfile('file')
await aerofs.os.path.isdir('folder')

# Get absolute path
abs_path = await aerofs.os.path.abspath('relative/path')

# Get size
size = await aerofs.os.path.getsize('bigfile.iso')
```

## Stats and Metadata

Access file statistics asynchronously:

```python
stat = await aerofs.os.stat('example.txt')
print(f"Size: {stat.st_size}")
print(f"Modified: {stat.st_mtime}")
```
