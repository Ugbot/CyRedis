#!/usr/bin/env python3
"""Check that every script in examples/ imports against the installed package.

Two layers, so the check is meaningful both in the wheel-only packaging job
and in a full development checkout:

1. Every ``from cy_redis... import name`` in an example must resolve against
   the *installed* ``cy_redis``. This is the public-API contract the examples
   document, and it never depends on optional packages.
2. If every third-party module the example imports at top level is available
   (``fastapi``, ``cyredis_experimental``, ...), the example is imported for
   real so syntax errors and module-level mistakes surface. Otherwise it is
   reported as skipped together with the missing module, never silently.

Exit status is non-zero if any example fails either layer. Run it from
outside the source tree (or after ``pip install``) so the checkout cannot
shadow the installed package::

    python scripts/check_examples.py [--examples-dir examples]
"""

from __future__ import annotations

import argparse
import ast
import importlib
import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

PACKAGE = "cy_redis"
STDLIB = set(sys.stdlib_module_names) if hasattr(sys, "stdlib_module_names") else None


@dataclass
class ExampleReport:
    path: Path
    api_errors: List[str] = field(default_factory=list)
    import_error: Optional[str] = None
    missing_optional: Set[str] = field(default_factory=set)

    @property
    def ok(self) -> bool:
        return not self.api_errors and self.import_error is None

    @property
    def skipped(self) -> bool:
        return self.ok and bool(self.missing_optional)


def _top_level_imports(tree: ast.Module) -> Iterable[Tuple[str, Optional[str]]]:
    """Yield ``(module, name)`` for module-level imports. ``name`` is None for
    ``import x`` and the imported symbol for ``from x import name``."""
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, None
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            for alias in node.names:
                yield node.module, alias.name


def _is_stdlib(module: str) -> bool:
    root = module.split(".")[0]
    if STDLIB is not None:
        return root in STDLIB
    spec = importlib.util.find_spec(root)
    return (
        spec is not None
        and spec.origin is not None
        and "site-packages" not in spec.origin
    )


def _check_api(module: str, name: Optional[str]) -> Optional[str]:
    try:
        mod = importlib.import_module(module)
    except Exception as exc:  # noqa: BLE001 - report whatever broke the import
        return f"{module}: {type(exc).__name__}: {exc}"
    if name is None or name == "*":
        return None
    if hasattr(mod, name):
        return None
    try:
        importlib.import_module(f"{module}.{name}")
    except ImportError:
        return f"{module}.{name} does not exist in the installed package"
    return None


def check_example(path: Path) -> ExampleReport:
    report = ExampleReport(path)
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        report.import_error = f"SyntaxError: {exc}"
        return report

    for module, name in _top_level_imports(tree):
        root = module.split(".")[0]
        if root == PACKAGE:
            error = _check_api(module, name)
            if error:
                report.api_errors.append(error)
        elif not _is_stdlib(module) and importlib.util.find_spec(root) is None:
            report.missing_optional.add(root)

    if report.api_errors or report.missing_optional:
        return report

    spec = importlib.util.spec_from_file_location(f"_example_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module_obj = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module_obj)
    except Exception as exc:  # noqa: BLE001 - report whatever broke the import
        report.import_error = f"{type(exc).__name__}: {exc}"
    return report


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--examples-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "examples",
    )
    args = parser.parse_args(argv)

    paths = sorted(p for p in args.examples_dir.glob("*.py") if p.name != "__init__.py")
    if not paths:
        print(f"no examples found under {args.examples_dir}", file=sys.stderr)
        return 1

    installed = importlib.import_module(PACKAGE)
    print(f"checking {len(paths)} examples against {PACKAGE} from {installed.__file__}")

    failures = 0
    for path in paths:
        report = check_example(path)
        if not report.ok:
            failures += 1
            print(f"FAIL  {path.name}")
            for error in report.api_errors:
                print(f"        {error}")
            if report.import_error:
                print(f"        {report.import_error}")
        elif report.skipped:
            needs = ", ".join(sorted(report.missing_optional))
            print(f"SKIP  {path.name}  (API ok; full import needs: {needs})")
        else:
            print(f"ok    {path.name}")

    print(f"{len(paths) - failures} passed, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
