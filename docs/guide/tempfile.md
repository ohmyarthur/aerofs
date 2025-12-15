# Temporary Files

The `aerofs.tempfile` module provides asynchronous equivalents to Python's `tempfile` module.

## NamedTemporaryFile

Creates a temporary file that has a visible name in the filesystem.

```python
import aerofs.tempfile

async with aerofs.tempfile.NamedTemporaryFile('w+') as f:
    await f.write("Temporary content")
    await f.seek(0)
    print(await f.read())
    
# File is automatically deleted on exit (unless delete=False)
```

## TemporaryDirectory

Creates a temporary directory.

```python
import aerofs.tempfile
import aerofs.os

async with aerofs.tempfile.TemporaryDirectory() as tmpdir:
    print(f"Created temp dir at: {tmpdir}")
    temp_file_path = f"{tmpdir}/test.txt"
    
    async with aerofs.open(temp_file_path, 'w') as f:
        await f.write("Data inside temp dir")
        
# Directory and contents are removed here
```

## SpooledTemporaryFile

Stores data in memory until the file size exceeds `max_size`, then spills to disk.

```python
async with aerofs.tempfile.SpooledTemporaryFile(max_size=1024, mode='w+') as f:
    await f.write("In memory...")
```
