# FAQ

## Why doesn't AeroFS use io_uring?

AeroFS intentionally uses Tokio's thread-pool-based async I/O instead of Linux's io_uring. This is a deliberate architectural decision:

### Deployment Compatibility
- **Docker blocks io_uring by default** via seccomp profiles
- Cloud environments often run older kernels (5.4, 5.10) with limited support
- Enterprise Linux like RHEL 8 has no io_uring support

### Security
- io_uring has significant CVE history (60% of kernel exploits in 2022)
- Google, Docker, and others have disabled it by default

### Stability
- AeroFS is designed as a drop-in replacement for Python's `open()`, `os`, and `tempfile`
- API stability and compatibility are prioritized over raw performance
- io_uring would introduce kernel-version-dependent behavior

### Performance Reality
- Performance difference is minimal for typical workloads
- io_uring benefits are most pronounced at >10,000 ops/sec
- Most Python applications are limited by GIL, not I/O

### Future Considerations
We will reconsider io_uring when:
- Docker enables it by default
- Tokio mainline integrates io_uring transparently
- 2+ years pass without major io_uring CVEs

For high-performance use cases requiring io_uring, consider specialized libraries with io_uring backends.
