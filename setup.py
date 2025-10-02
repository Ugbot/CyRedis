#!/usr/bin/env python3
"""
Setup script for building the CyRedis Cython extension.
Automatically detects and builds with hiredis on all platforms.
"""

from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
import os
import sys
import subprocess
import platform
import shutil

# Import Cython if available
try:
    from Cython.Distutils import build_ext as cython_build_ext
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False


class BuildRedisModule(build_py):
    """Custom command to build the Redis pgcache module"""

    def run(self):
        # Check if Redis module sources exist
        module_dir = os.path.join(os.path.dirname(__file__), 'cy_redis', 'pgcache')
        src_dir = os.path.join(module_dir, 'src')
        makefile = os.path.join(module_dir, 'Makefile')

        if not os.path.exists(src_dir) or not os.path.exists(makefile):
            print("Redis module sources not found, skipping Redis module build")
            return

        # Check for required dependencies
        if not self.check_redis_module_deps():
            print("Redis module dependencies not found, skipping build")
            print("Required: Redis dev headers, PostgreSQL dev headers, hiredis, jansson")
            return

        print("Building Redis pgcache module...")

        # Build the Redis module
        try:
            result = subprocess.run(['make', '-C', module_dir],
                                  capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                print("Failed to build Redis module:")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                raise subprocess.CalledProcessError(result.returncode, 'make')

            print("✓ Redis module built successfully")

        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            print(f"Redis module build failed: {e}")
            print("Redis module will not be available")
            return

        # Copy the built module to package data
        module_so = os.path.join(module_dir, 'pgcache.so')
        if os.path.exists(module_so):
            # Ensure build directory exists
            build_dir = os.path.join(self.build_lib, 'cy_redis', 'pgcache')
            os.makedirs(build_dir, exist_ok=True)

            # Copy the module
            shutil.copy2(module_so, build_dir)
            print(f"✓ Redis module copied to {build_dir}")

        super().run()

    def check_redis_module_deps(self):
        """Check if Redis module dependencies are available"""
        deps = [
            ('Redis', 'redis-server', '--version'),
            ('PostgreSQL', 'pg_config', '--version'),
            ('hiredis', 'pkg-config', '--exists hiredis'),
            ('jansson', 'pkg-config', '--exists jansson'),
        ]

        for name, cmd, arg in deps:
            try:
                if cmd == 'pkg-config':
                    result = subprocess.run([cmd, arg], capture_output=True, timeout=10)
                else:
                    result = subprocess.run([cmd, arg], capture_output=True, timeout=10)
                if result.returncode != 0:
                    return False
            except (subprocess.TimeoutExpired, FileNotFoundError):
                return False

        return True


class BuildExt(build_ext):
    """Custom build_ext command that checks for hiredis before building"""

    def run(self):
        # Check for hiredis before building
        if not self.check_hiredis():
            self.install_hiredis()
            if not self.check_hiredis():
                raise RuntimeError(
                    "hiredis library is required but could not be found or installed. "
                    "Please install hiredis manually:\n"
                    "  macOS: brew install hiredis\n"
                    "  Ubuntu/Debian: sudo apt-get install libhiredis-dev\n"
                    "  CentOS/RHEL: sudo yum install hiredis-devel\n"
                    "  Fedora: sudo dnf install hiredis-devel"
                )

        # Continue with normal build
        super().run()

    def check_hiredis(self):
        """Check if hiredis is available"""
        try:
            # Try pkg-config first
            result = subprocess.run(['pkg-config', '--exists', 'hiredis'],
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            pass

        # Try to compile a small test program
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False) as f:
            f.write("""
#include <hiredis/hiredis.h>
#include <stdio.h>
int main() {
    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c && !c->err) {
        redisFree(c);
        return 0;
    }
    return 1;
}
""")
            test_file = f.name

        try:
            result = subprocess.run(['cc', '-lhiredis', test_file, '-o', '/dev/null'],
                                  capture_output=True, timeout=10)
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            return False
        finally:
            try:
                os.unlink(test_file)
            except OSError:
                pass

    def install_hiredis(self):
        """Attempt to install hiredis automatically"""
        system = platform.system().lower()

        try:
            if system == "darwin":
                # macOS with Homebrew
                if subprocess.run(['which', 'brew'], capture_output=True).returncode == 0:
                    print("Installing hiredis via Homebrew...")
                    subprocess.run(['brew', 'install', 'hiredis'], check=True, timeout=300)
                    return True

            elif system == "linux":
                # Try different Linux package managers
                for cmd in [
                    ['apt-get', 'update', '&&', 'apt-get', 'install', '-y', 'libhiredis-dev'],
                    ['yum', 'install', '-y', 'hiredis-devel'],
                    ['dnf', 'install', '-y', 'hiredis-devel'],
                    ['pacman', '-S', '--noconfirm', 'hiredis'],
                ]:
                    try:
                        if subprocess.run(['which', cmd[0]], capture_output=True).returncode == 0:
                            print(f"Installing hiredis via {cmd[0]}...")
                            subprocess.run(cmd, check=True, timeout=300)
                            return True
                    except (subprocess.CalledProcessError, FileNotFoundError):
                        continue

        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            pass

        return False


def get_extension_config():
    """Get platform-specific extension configuration"""
    system = platform.system().lower()

    # Base configuration
    config = {
        'include_dirs': [],
        'library_dirs': [],
        'libraries': ['hiredis'],
        'extra_compile_args': [
            "-O3",
            "-fPIC",
            "-pthread",
        ],
        'extra_link_args': [
            "-pthread",
        ],
        'sources': [],  # Additional source files to compile
    }

    # Platform-specific adjustments
    if system == "darwin":
        # macOS
        config['include_dirs'].extend([
            '/usr/local/include',
            '/opt/homebrew/include',
        ])
        config['library_dirs'].extend([
            '/usr/local/lib',
            '/opt/homebrew/lib',
        ])
        config['extra_compile_args'].extend([
            "-mmacosx-version-min=10.9",
        ])
        config['extra_link_args'].extend([
            "-mmacosx-version-min=10.9",
        ])

    elif system == "linux":
        # Linux
        config['include_dirs'].extend([
            '/usr/include',
            '/usr/local/include',
        ])
        config['library_dirs'].extend([
            '/usr/lib',
            '/usr/lib64',
            '/usr/local/lib',
            '/usr/local/lib64',
        ])

    elif system == "windows":
        # Windows (experimental support)
        config['libraries'].append('ws2_32')
        config['extra_compile_args'].extend([
            "/MT",  # Static linking
        ])

    return config


# Get extension configuration
ext_config = get_extension_config()

# Handle .pyx files if Cython is available
if USE_CYTHON:
    # Use .pyx files directly
    source_ext = ".pyx"
    cmdclass = {'build_ext': cython_build_ext}
else:
    # Fall back to .c files (need to cythonize manually first)
    source_ext = ".c"
    cmdclass = {}

# Extension definitions
extensions = [
    Extension(
        "cy_redis.cy_redis_client",
        sources=["cy_redis/cy_redis_client" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.messaging",
        sources=["cy_redis/messaging" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.distributed",
        sources=["cy_redis/distributed" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.rpc",
        sources=["cy_redis/rpc" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.advanced",
        sources=["cy_redis/advanced" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'] + ['z'],  # Add zlib for compression
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.shared_dict",
        sources=["cy_redis/shared_dict" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'] + ['z'],  # Add zlib for compression
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    ),
    Extension(
        "cy_redis.script_manager",
        sources=["cy_redis/script_manager" + source_ext] + ext_config['sources'],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    )
]

# Check Python version
if sys.version_info < (3, 6):
    raise RuntimeError("CyRedis requires Python 3.6 or higher")

# Read README for long description
readme_path = os.path.join(os.path.dirname(__file__), 'README_CYREDIS.md')
long_description = ""
if os.path.exists(readme_path):
    with open(readme_path, 'r', encoding='utf-8') as f:
        long_description = f.read()

# Setup configuration
setup(
    name="cy-redis",
    version="0.1.0",
    description="High-performance threaded Redis client using Cython and hiredis with PostgreSQL caching",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="AI Assistant",
    author_email="",
    url="https://github.com/your-repo/cy-redis",
    packages=find_packages(),
    ext_modules=extensions,
    cmdclass=dict(cmdclass, **{
        'build_ext': BuildExt,
        'build_py': BuildRedisModule,
    }),
    package_data={
        'cy_redis': ['pgcache/*.so'],
        'cy_redis.pgcache': ['*.so', 'src/*', 'Makefile', 'build_module.sh', 'README.md'],
    },
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.6",
    install_requires=[
        "Cython>=0.29.0",
        "psycopg2-binary>=2.9.0",  # For PostgreSQL event handling
    ],
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-asyncio>=0.15",
            "black>=21.0",
            "flake8>=3.9",
        ],
        "docs": [
            "sphinx>=4.0",
            "sphinx-rtd-theme>=1.0",
        ],
        "postgres": [
            "psycopg2-binary>=2.9.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Cython",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Networking",
    ],
    keywords="redis cython hiredis high-performance threading async",
    project_urls={
        "Documentation": "https://github.com/your-repo/cy-redis",
        "Source": "https://github.com/your-repo/cy-redis",
        "Tracker": "https://github.com/your-repo/cy-redis/issues",
    },
)
