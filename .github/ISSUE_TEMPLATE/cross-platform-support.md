---
name: Cross-Platform Support
about: Add support for macOS and Windows platforms
title: '[FEATURE] Add macOS and Windows Support'
labels: enhancement, help wanted
assignees: ''

---

##  Feature Request

Currently, **aerofs** only supports **Linux**. We need to add support for **macOS** and **Windows** platforms.

## 📋 Current Status

-  **Linux**: Fully supported
-  **macOS**: Not supported
-  **Windows**: Not supported

## What Needs to be Done

### macOS Support
- [ ] Test compilation on macOS (Intel and Apple Silicon)
- [ ] Fix platform-specific file I/O issues
- [ ] Update CI/CD to build macOS wheels
- [ ] Test all features on macOS
- [ ] Update documentation

### Windows Support
- [ ] Test compilation on Windows
- [ ] Fix platform-specific file I/O issues (especially async file operations)
- [ ] Handle Windows-specific path separators and file modes
- [ ] Update CI/CD to build Windows wheels
- [ ] Test all features on Windows
- [ ] Update documentation

## Testing Requirements

Each platform should pass:
- [ ] All unit tests (`pytest`)
- [ ] File read/write operations
- [ ] Async iteration over files
- [ ] Tempfile operations
- [ ] OS operations (stat, listdir, etc.)
- [ ] Standard I/O operations

## Build Requirements

- [ ] Successfully build wheels for each platform
- [ ] Verify maturin compatibility
- [ ] Test wheel installation on each platform
- [ ] Ensure Rust dependencies work cross-platform

## Additional Context

### Potential Issues to Consider

**macOS:**
- Different file system (APFS vs ext4)
- Apple Silicon (ARM64) vs Intel compatibility
- macOS-specific file permissions

**Windows:**
- Different file paths (`\` vs `/`)
- File locking behavior
- Windows-specific file modes (`rb`, `wb+`, etc.)
- CRLF vs LF line endings

### References

- PyO3 cross-platform guide: https://pyo3.rs/
- Maturin multi-platform builds: https://www.maturin.rs/
- Tokio Windows support: https://tokio.rs/

## 🙋 Help Wanted

If you have access to macOS or Windows and would like to help with testing or development, please comment below!

### How to Contribute

1. Fork the repository
2. Set up development environment on your platform
3. Try to build and run tests
4. Report any issues you encounter
5. Submit PRs with fixes

---

**Note:** This is a significant enhancement that will make aerofs accessible to a wider audience. Any help is greatly appreciated! 🙏
