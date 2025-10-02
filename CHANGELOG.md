# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-09-28

### Added
- Initial release of CyRedis
- High-performance Cython Redis client using hiredis C library
- Threading support for concurrent operations
- Connection pooling for efficient resource management
- Async operations compatible with asyncio
- Stream processing with ThreadedStreamConsumer
- Cross-platform build support (macOS, Linux, Windows)
- Automatic hiredis dependency detection and installation
- Comprehensive documentation and examples
- Migration guide from aioredis

### Features
- Direct C bindings for maximum performance
- Multiple API layers (synchronous, asynchronous, threaded)
- Redis Streams support with high-throughput processing
- Type annotations and clean Python API
- Proper error handling and connection management

### Technical Details
- Cython extension wrapping hiredis C library
- Platform-specific compilation flags
- Modern Python packaging with pyproject.toml
- Automated build and install process
- Comprehensive test coverage
