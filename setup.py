"""Build configuration shim — pyproject.toml is the authoritative metadata.

setup.py exists for two build-time steps that cannot be expressed statically
in pyproject.toml:

1. Compile the vendored hiredis static library (``hiredis/libhiredis.a``)
   before the Cython extensions link against it. Every extension lists it in
   ``extra-objects``, so it must exist on a clean checkout / fresh sdist build.
2. Inject numpy's include directory into the ``cy_redis.features.ai`` extension
   (numpy's path is only known at build time).
"""
import os
import subprocess
import sys

from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext

HERE = os.path.abspath(os.path.dirname(__file__))
HIREDIS_DIR = os.path.join(HERE, "hiredis")
HIREDIS_STATIC_LIB = os.path.join(HIREDIS_DIR, "libhiredis.a")


class build_ext(_build_ext):
    """Build the vendored hiredis static lib, then compile the extensions."""

    def run(self):
        self._build_hiredis()
        super().run()

    def build_extensions(self):
        # numpy's include dir can't be a static string in pyproject.toml.
        try:
            import numpy

            np_inc = numpy.get_include()
            for ext in self.extensions:
                if ext.name == "cy_redis.features.ai" and np_inc not in ext.include_dirs:
                    ext.include_dirs.append(np_inc)
        except ImportError:
            # numpy missing → the ai extension fails to compile on its own,
            # with a clearer C error than a silent skip would give.
            pass
        super().build_extensions()

    def _build_hiredis(self):
        """Compile hiredis/libhiredis.a if it is missing.

        The extensions reference it via ``extra-objects``; without it every
        link step fails. We rebuild only when absent so incremental builds and
        editable installs stay fast.
        """
        if os.path.exists(HIREDIS_STATIC_LIB):
            return
        if not os.path.exists(os.path.join(HIREDIS_DIR, "Makefile")):
            raise SystemExit(
                "Cannot build the cy-redis C extensions: vendored hiredis "
                f"sources are missing from {HIREDIS_DIR}. This usually means "
                "the sdist was built without the hiredis/ tree — please report it."
            )
        print("building vendored hiredis static library (make -C hiredis static)")
        try:
            subprocess.check_call(["make", "static"], cwd=HIREDIS_DIR)
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            raise SystemExit(
                "Failed to build hiredis/libhiredis.a. A C toolchain and `make` "
                f"are required to build cy-redis from source. Underlying error: {exc}"
            )
        if not os.path.exists(HIREDIS_STATIC_LIB):
            raise SystemExit(
                "hiredis build completed but libhiredis.a was not produced; "
                "cannot continue."
            )


setup(cmdclass={"build_ext": build_ext})
