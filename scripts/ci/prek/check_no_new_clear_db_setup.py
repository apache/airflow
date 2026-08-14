#!/usr/bin/env python3
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
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.0.0",
# ]
# ///
"""Prevent new direct ``clear_db_*`` calls in fixture and xUnit setup."""

from __future__ import annotations

import argparse
import ast
import os
import subprocess
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, console

REPO_ROOT = AIRFLOW_ROOT_PATH
_ALLOWLIST_PATH = REPO_ROOT / "generated" / "known_clear_db_setup.txt"
_DB_MODULE = "tests_common.test_utils.db"
_XUNIT_SETUP_NAMES = {
    "setup",
    "setup_class",
    "setup_function",
    "setup_method",
    "setup_module",
}
_SPECIAL_PATHS = {
    "generated/known_clear_db_setup.txt",
    "scripts/ci/prek/check_no_new_clear_db_setup.py",
}


@dataclass(frozen=True)
class Violation:
    """One direct database cleanup call in a guarded setup phase."""

    line: int
    helper: str
    phase: str
    owner: str


@dataclass(frozen=True)
class _Bindings:
    helpers: dict[str, str]


@dataclass(frozen=True)
class _ImportEvent:
    line: int
    helper_aliases: tuple[tuple[str, str], ...] = ()


def _is_cleanup_helper(name: str) -> bool:
    return name == "clear_all" or name.startswith("clear_db_")


def _print_message(message: str) -> None:
    if console:
        console.print(message)
    else:
        print(message)


def _extract_import_event(node: ast.ImportFrom) -> _ImportEvent | None:
    helpers: list[tuple[str, str]] = []
    if isinstance(node, ast.ImportFrom) and node.module == _DB_MODULE:
        helpers.extend(
            (item.asname or item.name, item.name) for item in node.names if _is_cleanup_helper(item.name)
        )
    if not helpers:
        return None
    return _ImportEvent(node.lineno, tuple(helpers))


def _collect_module_bindings(tree: ast.Module) -> _Bindings:
    helpers: dict[str, str] = {}
    for statement in tree.body:
        if not isinstance(statement, ast.ImportFrom):
            continue
        event = _extract_import_event(statement)
        if event:
            helpers.update(event.helper_aliases)
    return _Bindings(helpers)


class _BodyVisitor(ast.NodeVisitor):
    """Visit a function body without entering nested execution scopes."""

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        return None

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return None


class _FunctionFacts(_BodyVisitor):
    def __init__(self) -> None:
        self.calls: list[ast.Call] = []
        self.yields: list[ast.Yield | ast.YieldFrom] = []
        self.imports: list[_ImportEvent] = []

    def visit_Call(self, node: ast.Call) -> None:
        self.calls.append(node)
        self.generic_visit(node)

    def visit_Yield(self, node: ast.Yield) -> None:
        self.yields.append(node)
        self.generic_visit(node)

    def visit_YieldFrom(self, node: ast.YieldFrom) -> None:
        self.yields.append(node)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if event := _extract_import_event(node):
            self.imports.append(event)


def _collect_function_facts(function: ast.FunctionDef | ast.AsyncFunctionDef) -> _FunctionFacts:
    facts = _FunctionFacts()
    for statement in function.body:
        facts.visit(statement)
    return facts


def _get_bindings_at(line: int, module: _Bindings, imports: list[_ImportEvent]) -> _Bindings:
    helpers = dict(module.helpers)
    for event in imports:
        if event.line > line:
            continue
        helpers.update(event.helper_aliases)
    return _Bindings(helpers)


def _find_called_helper(call: ast.Call, bindings: _Bindings) -> str | None:
    if isinstance(call.func, ast.Name):
        return bindings.helpers.get(call.func.id)
    return None


def _extract_decorator_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Call):
        return _extract_decorator_name(node.func)
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_fixture(function: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any(_extract_decorator_name(decorator) == "fixture" for decorator in function.decorator_list)


def _collect_functions(
    statements: list[ast.stmt], prefix: tuple[str, ...] = ()
) -> list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]]:
    functions: list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef]] = []
    for statement in statements:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append((".".join((*prefix, statement.name)), statement))
        elif isinstance(statement, ast.ClassDef):
            functions.extend(_collect_functions(statement.body, (*prefix, statement.name)))
    return functions


def find_setup_db_cleanups(path: Path) -> list[Violation]:
    """Return direct cleanup calls in fixture setup and standard xUnit setup."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    module = _collect_module_bindings(tree)
    violations: list[Violation] = []
    for owner, function in _collect_functions(tree.body):
        fixture = _is_fixture(function)
        if not fixture and function.name not in _XUNIT_SETUP_NAMES:
            continue
        facts = _collect_function_facts(function)
        boundary = min((node.lineno for node in facts.yields), default=None) if fixture else None
        phase = "fixture setup" if fixture else "xunit setup"
        for call in facts.calls:
            if boundary is not None and call.lineno > boundary:
                continue
            helper = _find_called_helper(call, _get_bindings_at(call.lineno, module, facts.imports))
            if helper:
                violations.append(Violation(call.lineno, helper, phase, owner))
    return sorted(violations, key=lambda item: (item.line, item.owner, item.helper))


def _iter_python_files() -> list[Path]:
    files = list((REPO_ROOT / "airflow-core" / "tests" / "unit").rglob("*.py"))
    files.extend((REPO_ROOT / "providers").glob("*/**/tests/unit/**/*.py"))
    return sorted(set(files))


def _get_relative_path(path: Path) -> str:
    return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()


def _build_site_key(relative_path: str, violation: Violation) -> str:
    return "::".join((relative_path, violation.owner, violation.phase, violation.helper, str(violation.line)))


def _inspect_paths(paths: list[Path]) -> tuple[Counter[str], dict[str, list[Violation]]]:
    counts: Counter[str] = Counter()
    details: dict[str, list[Violation]] = {}
    for path in sorted(set(paths)):
        relative = _get_relative_path(path)
        violations = find_setup_db_cleanups(path)
        details[relative] = violations
        counts.update(_build_site_key(relative, violation) for violation in violations)
    return counts, details


def _serialize_allowlist(counts: Counter[str]) -> str:
    return "".join(f"{key}::{counts[key]}\n" for key in sorted(counts))


def _is_unit_test_path(relative_path: str) -> bool:
    path = Path(relative_path)
    if path.is_absolute() or ".." in path.parts:
        return False
    parts = path.parts
    if parts[:3] == ("airflow-core", "tests", "unit"):
        return True
    return bool(
        len(parts) > 4
        and parts[0] == "providers"
        and any(parts[index : index + 2] == ("tests", "unit") for index in range(2, len(parts) - 1))
    )


def _parse_allowlist(text: str, *, allow_legacy_format: bool = False) -> Counter[str]:
    counts: Counter[str] = Counter()
    for line in text.splitlines():
        try:
            key, raw_count = line.rsplit("::", 1)
            count = int(raw_count)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid entry: {line!r}") from error
        parts = key.split("::")
        if len(parts) == 5:
            relative_path, owner, phase, helper, raw_line = parts
            try:
                source_line = int(raw_line)
            except ValueError as error:
                raise ValueError(f"invalid entry: {line!r}") from error
        elif allow_legacy_format and len(parts) == 4:
            relative_path, owner, phase, helper = parts
            source_line = 1
        else:
            raise ValueError(f"invalid entry: {line!r}")
        phases = {"fixture setup", "xunit setup"}
        if allow_legacy_format:
            phases.add("test prefix")
        if not _is_unit_test_path(relative_path) or not owner or phase not in phases:
            raise ValueError(f"invalid entry: {line!r}")
        if not _is_cleanup_helper(helper) or source_line <= 0 or count <= 0 or key in counts:
            raise ValueError(f"invalid entry: {line!r}")
        counts[key] = count
    if text != _serialize_allowlist(counts):
        raise ValueError("entries must be sorted, unique, and canonical")
    return counts


def _read_allowlist(path: Path) -> Counter[str]:
    return _parse_allowlist(path.read_text(encoding="utf-8"))


def _run_git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, text=True, capture_output=True, check=False)


def _base_predates_hook(baseline_ref: str) -> bool:
    resolved = _run_git("rev-parse", "--verify", f"{baseline_ref}^{{commit}}")
    if resolved.returncode:
        raise RuntimeError(f"invalid baseline ref {baseline_ref}: {resolved.stderr.strip()}")
    checker_path = "scripts/ci/prek/check_no_new_clear_db_setup.py"
    checker = _run_git("ls-tree", "--name-only", baseline_ref, "--", checker_path)
    if checker.returncode:
        raise RuntimeError(f"could not inspect baseline ref {baseline_ref}: {checker.stderr.strip()}")
    return not checker.stdout.strip()


def _load_trusted_allowlist() -> Counter[str] | None:
    relative = _get_relative_path(_ALLOWLIST_PATH)
    baseline_ref = os.environ.get("PRE_COMMIT_FROM_REF")
    if baseline_ref:
        result = _run_git("show", f"{baseline_ref}:{relative}")
        if result.returncode:
            if _base_predates_hook(baseline_ref):
                return None
            raise RuntimeError(
                f"could not read trusted baseline from {baseline_ref}: {result.stderr.strip()}"
            )
        return _parse_allowlist(result.stdout, allow_legacy_format=True)

    changed = _run_git("diff", "--quiet", "HEAD", "--", relative)
    if changed.returncode == 0:
        return None
    if changed.returncode != 1:
        raise RuntimeError(f"could not compare trusted baseline: {changed.stderr.strip()}")
    result = _run_git("show", f"HEAD:{relative}")
    if result.returncode:
        if _base_predates_hook("HEAD"):
            return None
        raise RuntimeError(f"could not read trusted baseline from HEAD: {result.stderr.strip()}")
    return _parse_allowlist(result.stdout, allow_legacy_format=True)


def _reject_allowlist_growth(current: Counter[str], trusted: Counter[str]) -> list[str]:
    if any(len(key.split("::")) == 4 for key in trusted):
        # One-time migration from the unpublished semantic baseline to line-exact identities.
        # Collapse both sides only while the trusted revision still uses the legacy format.
        collapsed: Counter[str] = Counter()
        for key, count in current.items():
            collapsed["::".join(key.split("::")[:4])] += count
        current = collapsed
    return sorted(key for key, count in current.items() if count > trusted.get(key, 0))


def _select_paths_to_check(filenames: list[str], all_files: bool) -> tuple[list[Path], bool]:
    if all_files:
        return _iter_python_files(), True
    paths = [Path(filename).resolve() for filename in filenames]
    relative_paths = {_get_relative_path(path) for path in paths}
    if relative_paths & _SPECIAL_PATHS:
        return _iter_python_files(), True
    return [path for path in paths if path.suffix == ".py"], False


def _print_violation_details(keys: list[str], details: dict[str, list[Violation]], *, heading: str) -> None:
    _print_message(heading)
    wanted = set(keys)
    for relative_path, violations in sorted(details.items()):
        for violation in violations:
            if _build_site_key(relative_path, violation) in wanted:
                _print_message(
                    f"  {relative_path}:{violation.line}: {violation.helper}() "
                    f"({violation.phase}, {violation.owner})"
                )


def _check_paths(paths: list[Path], allowed: Counter[str], *, full_scope: bool) -> int:
    actual, details = _inspect_paths(paths)
    relative_paths = {_get_relative_path(path) for path in paths}
    relevant_allowed = (
        allowed
        if full_scope
        else Counter(
            {key: count for key, count in allowed.items() if key.split("::", 1)[0] in relative_paths}
        )
    )
    added = sorted(key for key, count in actual.items() if count > relevant_allowed.get(key, 0))
    stale = sorted(key for key, count in relevant_allowed.items() if count > actual.get(key, 0))
    if added:
        _print_violation_details(
            added, details, heading="New setup-time database pre-cleaning is not allowed:"
        )
        _print_message("Fix teardown in the owning fixture instead of cleaning before the next test.")
    if stale:
        _print_message("The allowlist has stale setup-cleanup entries; run this hook with --generate:")
        for key in stale:
            _print_message(f"  {key}")
    return int(bool(added or stale))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="*", metavar="FILE")
    parser.add_argument("--all-files", action="store_true")
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args(argv)

    try:
        if not args.files and not args.all_files and not args.generate:
            return 0
        if args.generate:
            counts, _ = _inspect_paths(_iter_python_files())
            _ALLOWLIST_PATH.write_text(_serialize_allowlist(counts), encoding="utf-8")
            _print_message(
                f"Wrote {len(counts)} setup-cleanup identities to {_get_relative_path(_ALLOWLIST_PATH)}."
            )
            return 0

        allowed = _read_allowlist(_ALLOWLIST_PATH)
        trusted = _load_trusted_allowlist()
        if trusted is not None and (growth := _reject_allowlist_growth(allowed, trusted)):
            _print_message("The allowlist cannot add or increase setup-cleanup identities:")
            for key in growth:
                _print_message(f"  {key}")
            return 1
        paths, full_scope = _select_paths_to_check(args.files, args.all_files)
        return _check_paths(paths, allowed, full_scope=full_scope)
    except ValueError as error:
        _print_message(f"Setup-cleanup allowlist is invalid: {error}")
        return 1
    except (OSError, SyntaxError, UnicodeError) as error:
        filename = getattr(error, "filename", None)
        location = f" ({filename})" if filename else ""
        _print_message(f"Setup-cleanup check failed closed{location}: {error}")
        return 1
    except RuntimeError as error:
        _print_message(f"Could not establish a trusted baseline: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
