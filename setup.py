"""Build configuration shim — pyproject.toml is the authoritative metadata.

setup.py exists for build-time steps that cannot be expressed statically in
pyproject.toml:

1. Compile the vendored hiredis static libraries (``hiredis/libhiredis.a``
   and, when OpenSSL is available, ``hiredis/libhiredis_ssl.a``) before the
   Cython extensions link against them. Every extension lists libhiredis.a in
   ``extra-objects``, so it must exist on a clean checkout / fresh sdist build.
2. Drop the ``cy_redis.core.tls_support`` extension when OpenSSL development
   headers are missing, so plain-TCP installs still succeed anywhere. The
   client raises a clear error at runtime if TLS is requested on such a build.
3. Inject numpy's include directory into the ``cy_redis.features.ai``
   extension and OpenSSL's include/lib directories into ``tls_support``
   (their paths are only known at build time).
"""
import os
import subprocess
import sys

from setuptools import setup
from setuptools.command.build_ext import build_ext as _build_ext

HERE = os.path.abspath(os.path.dirname(__file__))
HIREDIS_DIR = os.path.join(HERE, "hiredis")
HIREDIS_STATIC_LIB = os.path.join(HIREDIS_DIR, "libhiredis.a")
HIREDIS_SSL_STATIC_LIB = os.path.join(HIREDIS_DIR, "libhiredis_ssl.a")
TLS_EXTENSION_NAME = "cy_redis.core.tls_support"


def _find_openssl_prefix():
    """Locate an OpenSSL installation prefix, or None for system default.

    Mirrors the vendored hiredis Makefile: OPENSSL_PREFIX env wins, then the
    Homebrew locations on macOS. On Linux the system paths need no prefix.
    """
    env_prefix = os.environ.get("OPENSSL_PREFIX") or os.environ.get("OPENSSL_ROOT_DIR")
    if env_prefix and os.path.isdir(env_prefix):
        return env_prefix
    if sys.platform == "darwin":
        for candidate in (
            "/opt/homebrew/opt/openssl",
            "/opt/homebrew/opt/openssl@3",
            "/usr/local/opt/openssl",
            "/usr/local/opt/openssl@3",
        ):
            if os.path.isdir(candidate):
                return candidate
    return None


class build_ext(_build_ext):
    """Build the vendored hiredis static libs, then compile the extensions."""

    def run(self):
        self.hiredis_ssl_available = self._build_hiredis()
        super().run()

    def build_extensions(self):
        if not self.hiredis_ssl_available:
            # No OpenSSL at build time: ship everything except TLS support.
            # cy_redis raises a descriptive error if use_tls=True on this build.
            print(
                "warning: building cy-redis WITHOUT TLS support "
                "(libhiredis_ssl.a could not be built — install OpenSSL "
                "development headers to enable it)"
            )
            self.extensions = [
                ext for ext in self.extensions if ext.name != TLS_EXTENSION_NAME
            ]

        openssl_prefix = _find_openssl_prefix()
        try:
            import numpy

            np_inc = numpy.get_include()
        except ImportError:
            # numpy missing → the ai extension fails to compile on its own,
            # with a clearer C error than a silent skip would give.
            np_inc = None

        for ext in self.extensions:
            if ext.name == "cy_redis.features.ai" and np_inc and np_inc not in ext.include_dirs:
                ext.include_dirs.append(np_inc)
            if ext.name == TLS_EXTENSION_NAME and openssl_prefix:
                ext.include_dirs.append(os.path.join(openssl_prefix, "include"))
                ext.library_dirs.append(os.path.join(openssl_prefix, "lib"))
        super().build_extensions()

    def _build_hiredis(self):
        """Compile the vendored hiredis static libraries if missing.

        Returns True when libhiredis_ssl.a is available (TLS build), False
        when only the plain libhiredis.a could be built. The core archive is
        mandatory — every extension links it — so its absence is fatal; the
        SSL archive is best-effort.
        """
        if os.path.exists(HIREDIS_STATIC_LIB) and os.path.exists(HIREDIS_SSL_STATIC_LIB):
            return True
        if not os.path.exists(os.path.join(HIREDIS_DIR, "Makefile")):
            raise SystemExit(
                "Cannot build the cy-redis C extensions: vendored hiredis "
                f"sources are missing from {HIREDIS_DIR}. This usually means "
                "the sdist was built without the hiredis/ tree — please report it."
            )

        make_env = dict(os.environ)
        openssl_prefix = _find_openssl_prefix()
        if openssl_prefix:
            make_env.setdefault("OPENSSL_PREFIX", openssl_prefix)

        # Preferred: one build that produces both archives.
        print("building vendored hiredis static libraries (make -C hiredis static USE_SSL=1)")
        try:
            subprocess.check_call(
                ["make", "static", "USE_SSL=1"], cwd=HIREDIS_DIR, env=make_env
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Most likely OpenSSL headers are absent. Fall back to a plain
            # build; only the TLS extension is lost.
            print("hiredis SSL build failed; retrying without SSL (TLS support disabled)")
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
        return os.path.exists(HIREDIS_SSL_STATIC_LIB)


setup(cmdclass={"build_ext": build_ext})
