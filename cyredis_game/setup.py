#!/usr/bin/env python3
"""
Setup script for CyRedis Game Engine
A distributed game server platform built on CyRedis
"""

import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Check Python version
if sys.version_info < (3, 6):
    raise RuntimeError("CyRedis Game Engine requires Python 3.6 or higher")

# Determine if we should use Cython
USE_CYTHON = os.environ.get('USE_CYTHON', '1') == '1'

try:
    from Cython.Build import cythonize
    USE_CYTHON = True
except ImportError:
    if USE_CYTHON:
        print("Cython not available, building from C files")
        USE_CYTHON = False

# Source file extension
source_ext = '.pyx' if USE_CYTHON else '.c'

# Get extension configuration (inherited from base CyRedis)
def get_extension_config():
    """Get compiler/linker configuration for hiredis and dependencies"""

    # Check for vendored hiredis
    hiredis_path = os.path.join(os.path.dirname(__file__), '..', 'hiredis')
    if os.path.exists(hiredis_path):
        print("✓ Using vendored hiredis")
        include_dirs = [os.path.join(hiredis_path, 'include')]
        library_dirs = [os.path.join(hiredis_path, 'lib')]
        libraries = ['hiredis']
        extra_compile_args = ['-O3', '-march=native']
        extra_link_args = []
    else:
        print("⚠ Using system hiredis (if available)")
        include_dirs = []
        library_dirs = []
        libraries = ['hiredis']
        extra_compile_args = ['-O3', '-march=native']
        extra_link_args = []

    return {
        'include_dirs': include_dirs,
        'library_dirs': library_dirs,
        'libraries': libraries,
        'extra_compile_args': extra_compile_args,
        'extra_link_args': extra_link_args,
    }

# Get configuration
ext_config = get_extension_config()

# Custom build extension to handle Cython
class build_ext(_build_ext):
    def finalize_options(self):
        _build_ext.finalize_options(self)
        if self.distribution.ext_modules:
            for ext in self.distribution.ext_modules:
                ext.cython_directives = {
                    'language_level': 3,
                    'boundscheck': False,
                    'wraparound': False,
                    'cdivision': True,
                    'nonecheck': False,
                }

# Extension definitions
extensions = [
    Extension(
        "cyredis_game.game_engine",
        sources=["cyredis_game/game_engine" + source_ext],
        include_dirs=ext_config['include_dirs'],
        library_dirs=ext_config['library_dirs'],
        libraries=ext_config['libraries'],
        extra_compile_args=ext_config['extra_compile_args'],
        extra_link_args=ext_config['extra_link_args'],
        language="c",
    )
]

# Cythonize if available
if USE_CYTHON:
    extensions = cythonize(
        extensions,
        compiler_directives={
            'language_level': 3,
            'boundscheck': False,
            'wraparound': False,
            'cdivision': True,
            'nonecheck': False,
        }
    )

# Read README
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

# Setup configuration
setup(
    name="cyredis-game",
    version="0.1.0",
    description="Distributed game server platform built on CyRedis",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="CyRedis Team",
    author_email="cyredis@example.com",
    url="https://github.com/cyredis/cyredis-game",
    license="MIT",

    packages=['cyredis_game'],
    package_dir={'cyredis_game': 'cyredis_game'},

    ext_modules=extensions,
    cmdclass={'build_ext': build_ext},

    python_requires=">=3.6",
    install_requires=[
        "cy-redis>=0.1.0",  # Depends on base CyRedis
        "cython>=0.29.0",
        "msgpack>=1.0.0",   # Fast binary serialization
    ],

    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Games/Entertainment",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Distributed Computing",
    ],

    keywords=[
        "redis",
        "game-engine",
        "distributed-systems",
        "ecs",
        "entity-component-system",
        "real-time",
        "multiplayer",
        "simulation",
        "authoritative-server",
        "cython",
        "hiredis",
    ],

    project_urls={
        "Documentation": "https://github.com/cyredis/cyredis-game#readme",
        "Source": "https://github.com/cyredis/cyredis-game",
        "Tracker": "https://github.com/cyredis/cyredis-game/issues",
    },

    zip_safe=False,
)
