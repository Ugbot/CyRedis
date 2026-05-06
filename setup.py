"""
Minimal setup.py shim — pyproject.toml is the authoritative configuration.
This file exists only to inject numpy's dynamic include path into the
cy_redis.features.ai extension at build time, which cannot be a static
string in pyproject.toml.
"""
from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext


class build_ext(_build_ext):
    """Inject numpy include directory before compiling extensions."""

    def build_extensions(self):
        try:
            import numpy
            np_inc = numpy.get_include()
            for ext in self.extensions:
                if ext.name == "cy_redis.features.ai":
                    if np_inc not in ext.include_dirs:
                        ext.include_dirs.append(np_inc)
        except ImportError:
            pass  # numpy not installed; ai extension will fail on its own
        super().build_extensions()


setup(cmdclass={"build_ext": build_ext})
