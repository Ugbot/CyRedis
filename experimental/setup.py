"""Build the experimental Cython extensions against the in-tree cy_redis.

The experimental tree is never published. It compiles against the sibling
``cy_redis`` checkout (for ``cimport``-able ``.pxd`` files) and the vendored
hiredis static library that the main ``setup.py`` produces, so run

    uv pip install -e ..            # builds hiredis/libhiredis.a
    uv pip install -e .             # from experimental/

in that order.
"""

import os

from Cython.Build import cythonize
from setuptools import Extension, setup

HERE = os.path.abspath(os.path.dirname(__file__))
REPO = os.path.dirname(HERE)
HIREDIS_DIR = os.path.join(REPO, "hiredis")
HIREDIS_STATIC_LIB = os.path.join(HIREDIS_DIR, "libhiredis.a")

if not os.path.exists(HIREDIS_STATIC_LIB):
    raise SystemExit(
        f"{HIREDIS_STATIC_LIB} is missing. Build the main package first "
        "(`uv pip install -e ..` from this directory) so the vendored hiredis "
        "archive exists."
    )

C_MODULES = [
    "auth.token_manager",
    "auth.session_manager",
    "auth.two_factor_auth",
    "auth.password_reset_manager",
    "workers.worker_queue",
    "workers.lifecycle_manager",
    "workers.worker_coordinator",
    "workers.multi_session_tracker",
    "web.shared_state_manager",
    "web.web_cache",
    "web.web_app_support",
    "web.channels",
    "communication.messaging",
    "communication.rpc",
    "game.game_engine",
    "game.module_manager",
    "game.pathfinding",
    "game.physics",
    "game.goap",
    "game.chain",
]

CPP_MODULES = ["extras.probabilistic", "extras.ai"]


def _ext(module, **kwargs):
    path = module.replace(".", "/")
    include_dirs = [HERE, REPO, HIREDIS_DIR] + kwargs.pop("include_dirs", [])
    return Extension(
        f"cyredis_experimental.{module}",
        sources=[f"cyredis_experimental/{path}.pyx"],
        include_dirs=include_dirs,
        extra_objects=[HIREDIS_STATIC_LIB],
        **kwargs,
    )


extensions = [_ext(m) for m in C_MODULES]
extensions.append(_ext("extras.advanced", libraries=["z"]))

try:
    import numpy

    numpy_include = [numpy.get_include()]
except ImportError:
    numpy_include = []

for module in CPP_MODULES:
    extensions.append(
        _ext(
            module,
            language="c++",
            extra_compile_args=["-std=c++14"],
            include_dirs=[os.path.join(HERE, "cyredis_experimental/extras/cpp")]
            + numpy_include,
        )
    )

setup(
    ext_modules=cythonize(
        extensions,
        include_path=[REPO],
        compiler_directives={"language_level": "3"},
        nthreads=os.cpu_count() or 1,
    )
)
