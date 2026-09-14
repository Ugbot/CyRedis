"""Fail if a built wheel or sdist ships anything outside the supported surface.

Usage: python scripts/check_dist_contents.py dist/*.whl dist/*.tar.gz
"""

import sys
import tarfile
import zipfile

# Top-level directories/packages that must never appear in a distribution.
FORBIDDEN_ROOTS = ("experimental", "cyredis_experimental", "cyredis_game", "plugins")

# Modules that were deleted or moved out of cy_redis; a stale checkout or
# build directory could otherwise smuggle them back in.
FORBIDDEN_MODULES = (
    "cy_redis/web/",
    "cy_redis/auth/",
    "cy_redis/workers/",
    "cy_redis/communication/",
    "cy_redis/integrations/",
    "cy_redis/cpp/",
    "cy_redis/core/redis_core",
    "cy_redis/features/advanced",
    "cy_redis/features/probabilistic",
    "cy_redis/features/ai",
    "cy_redis/data/concurrent_shared_dict",
    "cy_redis/data/shared_state_manager",
    "cy_redis/cy_redis_client.py",
    "cy_redis/distributed.py",
    "cy_redis/high_performance_redis.py",
    "cy_redis/reliable_queue.py",
    "cy_redis/web_app_support.py",
)

FORBIDDEN_SUFFIXES = (".dylib", ".o", ".a", ".pyc")

REQUIRED_WHEEL_MODULES = (
    "cy_redis/core/cy_redis_client",
    "cy_redis/core/cluster",
    "cy_redis/core/sentinel",
    "cy_redis/core/tls_support",
    "cy_redis/core/protocol",
    "cy_redis/features/distributed",
    "cy_redis/features/functions",
    "cy_redis/features/json_ops",
    "cy_redis/features/search",
    "cy_redis/features/graph",
    "cy_redis/features/capabilities",
    "cy_redis/features/script_manager",
    "cy_redis/data/shared_dict",
    "cy_redis/utils/redis_iterators",
    "cy_redis/lua_scripts/",
)


def members(path):
    if path.endswith(".whl"):
        with zipfile.ZipFile(path) as zf:
            return zf.namelist()
    with tarfile.open(path) as tf:
        # sdist members are prefixed with "<name>-<version>/"
        return [m.name.split("/", 1)[1] for m in tf.getmembers() if "/" in m.name]


def check(path):
    names = members(path)
    problems = []
    for name in names:
        root = name.split("/", 1)[0]
        if root in FORBIDDEN_ROOTS:
            problems.append(f"forbidden tree: {name}")
        if any(name.startswith(prefix) for prefix in FORBIDDEN_MODULES):
            problems.append(f"moved/deleted module: {name}")
        if name.endswith(FORBIDDEN_SUFFIXES):
            problems.append(f"build artifact: {name}")
        if path.endswith(".whl") and name.endswith(".pyx"):
            problems.append(f"Cython source in wheel: {name}")
    if path.endswith(".whl"):
        for module in REQUIRED_WHEEL_MODULES:
            if not any(name.startswith(module) for name in names):
                problems.append(f"missing supported module: {module}")
    return problems


def main(paths):
    failed = False
    for path in paths:
        problems = check(path)
        status = "FAIL" if problems else "ok"
        print(f"{status}: {path}")
        for problem in problems:
            print(f"  - {problem}")
        failed = failed or bool(problems)
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1:]))
