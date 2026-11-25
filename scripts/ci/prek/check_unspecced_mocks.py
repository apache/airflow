#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "rich>=13.0.0",
# ]
# ///
"""Check that no new mocks without ``spec``/``autospec`` are added to test files.

The project convention is to use ``spec``/``autospec`` when mocking, so a mock
rejects attributes and call signatures that the real object does not have. A
violation is a call that creates a mock without a spec:

* ``Mock``, ``MagicMock``, ``AsyncMock``, ``NonCallableMock`` or
  ``NonCallableMagicMock`` without ``spec``/``spec_set`` or a positional spec.
* ``patch`` or ``patch.object`` without ``spec``, ``spec_set``, ``autospec``,
  ``new`` or ``new_callable``, given by keyword or by position.

An argument set to its "no spec" value counts as missing: ``None`` or ``False``
for ``spec``, ``spec_set`` and ``autospec``, ``DEFAULT`` for ``new``, and
``None`` for ``new_callable``. Calls are matched by name, so ``patch`` called
through a renamed module (``async_mock.patch(...)``) and a mock class imported
under another name (``MagicMock as MM``) are not counted.

All *existing* violations are recorded in ``generated/known_unspecced_mocks.txt``
as ``relative/path::N`` entries (one per file), where ``N`` is the maximum
number of mocks without a spec allowed in that file. A file whose current count
exceeds the recorded limit is treated as a violation – pass ``spec=`` or
``autospec=True`` instead.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist – just stage
    ``generated/known_unspecced_mocks.txt`` and re-run.

    A file that cannot be read or parsed fails the hook and leaves its entry unchanged.

``--all-files``:
    Walk every ``.py`` file in a ``tests`` directory under the project roots
    listed in ``_PROJECT_ROOTS`` — the same scope the pre-commit hook applies to.

``--cleanup``:
    Remove entries for files that no longer exist. Safe to run at any time;
    does not add new entries or raise limits.

``--generate``:
    Scan the same files as ``--all-files`` and *rebuild* the allowlist from
    scratch. Intended for the initial setup or after a large-scale clean-up sprint.
"""

from __future__ import annotations

import argparse
import ast
import textwrap
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import NamedTuple

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

# Top-level directories scanned by ``--all-files`` / ``--generate``. Keep in sync with the
# ``files:`` pattern for this hook in ``.pre-commit-config.yaml``.
_PROJECT_ROOTS = (
    "airflow-core",
    "airflow-ctl",
    "airflow-ctl-tests",
    "airflow-e2e-tests",
    "chart",
    "dev",
    "devel-common",
    "docker-tests",
    "kubernetes-tests",
    "performance",
    "providers",
    "scripts",
    "shared",
    "task-sdk",
    "task-sdk-integration-tests",
)

_EXCLUDED_DIRS = frozenset({".build", "node_modules", "site-packages"})

_DEFAULT_NAMES = frozenset(
    {
        "DEFAULT",
        "mock.DEFAULT",
        "unittest.mock.DEFAULT",
        "mocker.DEFAULT",
        "sentinel.DEFAULT",
        "mock.sentinel.DEFAULT",
    }
)

_MOCK_CLASSES = frozenset({"Mock", "MagicMock", "AsyncMock", "NonCallableMock", "NonCallableMagicMock"})


class _SpecRule(NamedTuple):
    target_arg_count: int
    positional_params: tuple[str, ...]
    spec_params: frozenset[str]


_PATCH_PARAMS = ("new", "spec", "create", "spec_set", "autospec", "new_callable")
_PATCH_SPEC_OR_NEW_PARAMS = frozenset(_PATCH_PARAMS) - {"create"}

_MOCK_CLASS_RULE = _SpecRule(0, ("spec",), frozenset({"spec", "spec_set"}))
_PATCH_RULE = _SpecRule(1, _PATCH_PARAMS, _PATCH_SPEC_OR_NEW_PARAMS)
_PATCH_OBJECT_RULE = _SpecRule(2, _PATCH_PARAMS, _PATCH_SPEC_OR_NEW_PARAMS)


def _get_dotted_name(node: ast.expr) -> str | None:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not isinstance(node, ast.Name):
        return None
    parts.append(node.id)
    return ".".join(reversed(parts))


def _get_spec_rule(name: str) -> _SpecRule | None:
    parts = name.split(".")
    if parts[-1] in _MOCK_CLASSES:
        return _MOCK_CLASS_RULE
    if parts == ["patch"] or parts[-2:] in (["mock", "patch"], ["mocker", "patch"]):
        return _PATCH_RULE
    if parts[-2:] == ["patch", "object"]:
        return _PATCH_OBJECT_RULE
    return None


def _is_none_or_false(value: ast.expr) -> bool:
    return isinstance(value, ast.Constant) and (value.value is None or value.value is False)


def _is_spec_or_new_argument(param: str, value: ast.expr) -> bool:
    if param == "new":
        return _get_dotted_name(value) not in _DEFAULT_NAMES
    return not _is_none_or_false(value)


def _is_unspecced(call: ast.Call, rule: _SpecRule) -> bool:
    if any(keyword.arg is None for keyword in call.keywords):
        return False
    arguments = dict(zip(rule.positional_params, call.args[rule.target_arg_count :]))
    arguments.update((keyword.arg, keyword.value) for keyword in call.keywords if keyword.arg)
    return not any(
        param in rule.spec_params and _is_spec_or_new_argument(param, value)
        for param, value in arguments.items()
    )


def _iter_unspecced_mock_calls(path: Path) -> Iterator[tuple[ast.Call, str]]:
    """Yield each call in *path* that creates a mock without a spec, with its dotted name."""
    source = path.read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(source, str(path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _get_dotted_name(node.func)
        if name is None:
            continue
        rule = _get_spec_rule(name)
        if rule is not None and _is_unspecced(node, rule):
            yield node, name


class UnspeccedMockAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _iter_test_files()

    def check(self, files: list[Path], allowlist: dict[str, int]) -> int:
        try:
            return super().check(files, allowlist)
        except (OSError, SyntaxError) as error:
            console.print(
                f"[red]Cannot check[/red] [cyan]{error.filename}[/cyan]: {error}. "
                "Fix it and re-run; the allowlist was not changed."
            )
            return 1

    def count_occurrences(self, path: Path) -> int:
        return sum(1 for _ in _iter_unspecced_mock_calls(path))

    def violation_panel_text(self) -> str:
        return textwrap.dedent(
            """\
            New [bold]mock without spec[/bold] detected.
            Pass [cyan]spec=[/cyan] to the mock class or [cyan]autospec=True[/cyan] to [cyan]patch[/cyan] so the mock
            rejects attributes and calls the real object does not have:

              [cyan]mock.MagicMock(spec=SomeClass)
              mock.patch("package.module.SomeClass.method", autospec=True)[/cyan]

            If this usage is intentional and pre-existing, run:

              [cyan]uv run ./scripts/ci/prek/check_unspecced_mocks.py --generate[/cyan]

            to regenerate the allowlist, then commit the updated
            [cyan]generated/known_unspecced_mocks.txt[/cyan]."""
        )

    def format_violation_details(self, path: Path) -> list[str]:
        calls = sorted(_iter_unspecced_mock_calls(path), key=lambda item: item[0].lineno)
        return [f"      [dim]L{call.lineno}[/dim] {name}(...)" for call, name in calls]


def _iter_test_files() -> list[Path]:
    candidates: list[Path] = []
    for top in _PROJECT_ROOTS:
        for path in (REPO_ROOT / top).rglob("*.py"):
            directories = path.relative_to(REPO_ROOT).parts[:-1]
            if "tests" in directories and _EXCLUDED_DIRS.isdisjoint(directories):
                candidates.append(path.resolve())
    return candidates


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new mocks without spec/autospec in test files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file in a tests directory under the project roots",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove stale entries from the allowlist and exit",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Regenerate the allowlist from the current codebase and exit",
    )
    args = parser.parse_args(argv)

    manager = UnspeccedMockAllowlistManager(REPO_ROOT / "generated" / "known_unspecced_mocks.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return manager.check(list(manager.iter_files()), allowlist)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    return manager.check([Path(f).resolve() for f in args.files], allowlist)


if __name__ == "__main__":
    raise SystemExit(main())
