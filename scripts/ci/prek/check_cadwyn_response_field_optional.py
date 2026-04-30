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
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
Guard against a Cadwyn response-downgrade footgun on the execution API.

When a Cadwyn ``VersionChange`` carries a
``@convert_response_to_previous_version_for(...)`` method, its
``schema(X).field("Y").didnt_exist`` instructions describe response fields
that the matching body converter pops from the dict before the response
goes out. After the converter runs, FastAPI's ``serialize_response`` still
re-validates the body against the HEAD ``response_model`` (class ``X``).
If ``Y`` is required on ``X``, the post-pop body fails validation and the
older-version client receives ``500 ResponseValidationError`` even though
the endpoint executed successfully.

This check enforces that any field declared as ``didnt_exist`` inside a
``VersionChange`` that does response conversion has a default on its HEAD
class definition (so the post-pop body still validates). VersionChange
classes that only do request migrations are skipped.

If you intentionally added a downgrade for a field that must stay
required at HEAD, you need a different migration strategy (e.g. set the
field to a sentinel value in the body converter rather than popping it,
or accept that older clients can no longer call this endpoint).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
EXECUTION_API_ROOT = REPO_ROOT / "airflow-core" / "src" / "airflow" / "api_fastapi" / "execution_api"
VERSIONS_DIR = EXECUTION_API_ROOT / "versions"


def _from_imports(tree: ast.AST) -> dict[str, str]:
    """Map locally-bound name -> dotted module it was imported from."""
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                out[alias.asname or alias.name] = node.module
    return out


def _parse_didnt_exist(node: ast.AST) -> tuple[str, str] | None:
    """If ``node`` is ``schema(ClassName).field("field").didnt_exist``, return the pair."""
    if not (isinstance(node, ast.Attribute) and node.attr == "didnt_exist"):
        return None
    field_call = node.value
    if not (
        isinstance(field_call, ast.Call)
        and isinstance(field_call.func, ast.Attribute)
        and field_call.func.attr == "field"
        and len(field_call.args) == 1
        and isinstance(field_call.args[0], ast.Constant)
        and isinstance(field_call.args[0].value, str)
    ):
        return None
    schema_call = field_call.func.value
    if not (
        isinstance(schema_call, ast.Call)
        and isinstance(schema_call.func, ast.Name)
        and schema_call.func.id == "schema"
        and len(schema_call.args) == 1
        and isinstance(schema_call.args[0], ast.Name)
    ):
        return None
    return schema_call.args[0].id, field_call.args[0].value


def _has_response_converter(class_node: ast.ClassDef) -> bool:
    for stmt in class_node.body:
        if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in stmt.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            name = (
                target.id
                if isinstance(target, ast.Name)
                else target.attr
                if isinstance(target, ast.Attribute)
                else None
            )
            if name == "convert_response_to_previous_version_for":
                return True
    return False


def _collect_didnt_exist(class_node: ast.ClassDef) -> list[tuple[str, str, int]]:
    found: list[tuple[str, str, int]] = []
    for stmt in class_node.body:
        if not isinstance(stmt, ast.Assign):
            continue
        if not any(
            isinstance(t, ast.Name) and t.id == "instructions_to_migrate_to_previous_version"
            for t in stmt.targets
        ):
            continue
        for sub in ast.walk(stmt.value):
            parsed = _parse_didnt_exist(sub)
            if parsed:
                found.append((parsed[0], parsed[1], sub.lineno))
    return found


def _find_class(tree: ast.AST, name: str) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _annotation_has_field_default(ann: ast.AST) -> bool:
    """Detect ``Annotated[T, Field(default=..., default_factory=...)]``."""
    if not (isinstance(ann, ast.Subscript) and isinstance(ann.value, ast.Name) and ann.value.id == "Annotated"):
        return False
    slice_node = ann.slice
    elts = slice_node.elts if isinstance(slice_node, ast.Tuple) else [slice_node]
    for meta in elts[1:]:
        if (
            isinstance(meta, ast.Call)
            and isinstance(meta.func, ast.Name)
            and meta.func.id == "Field"
        ):
            for kw in meta.keywords:
                if kw.arg in ("default", "default_factory"):
                    return True
    return False


def _field_has_default(class_node: ast.ClassDef, field_name: str) -> bool | None:
    """``True`` if the field has a default; ``False`` if required; ``None`` if not declared on this class."""
    for stmt in class_node.body:
        if not (isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name)):
            continue
        if stmt.target.id != field_name:
            continue
        if stmt.value is not None:
            return True
        return _annotation_has_field_default(stmt.annotation)
    return None


def _resolve_module(module: str) -> Path | None:
    rel = Path(module.replace(".", "/") + ".py")
    for base in (REPO_ROOT / "airflow-core" / "src", REPO_ROOT):
        candidate = base / rel
        if candidate.is_file():
            return candidate
    return None


_tree_cache: dict[Path, ast.Module] = {}


def _load(path: Path) -> ast.Module | None:
    if path in _tree_cache:
        return _tree_cache[path]
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError as exc:
        print(f"⚠️  cannot parse {path}: {exc}", file=sys.stderr)
        return None
    _tree_cache[path] = tree
    return tree


def _check_file(path: Path) -> int:
    tree = _load(path)
    if tree is None:
        return 0
    imports = _from_imports(tree)
    errors = 0
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and _has_response_converter(node)):
            continue
        for class_name, field_name, lineno in _collect_didnt_exist(node):
            module = imports.get(class_name)
            target_tree: ast.Module | None
            if module is None:
                target_tree = tree
                target_path = path
            else:
                target_path = _resolve_module(module)
                if target_path is None:
                    print(
                        f"⚠️  {path}:{lineno}: cannot resolve `{module}` for `{class_name}` — skipping",
                        file=sys.stderr,
                    )
                    continue
                target_tree = _load(target_path)
                if target_tree is None:
                    continue
            class_def = _find_class(target_tree, class_name)
            if class_def is None:
                print(
                    f"⚠️  {path}:{lineno}: class `{class_name}` not found in {target_path} — skipping",
                    file=sys.stderr,
                )
                continue
            has_default = _field_has_default(class_def, field_name)
            if has_default is None:
                print(
                    f"⚠️  {path}:{lineno}: field `{class_name}.{field_name}` not declared in "
                    f"{target_path.relative_to(REPO_ROOT)} (inherited?) — skipping",
                    file=sys.stderr,
                )
                continue
            if not has_default:
                print(
                    f"❌ {path.relative_to(REPO_ROOT)}:{lineno}: VersionChange `{node.name}` "
                    f"strips `{class_name}.{field_name}` from a response, but the HEAD schema "
                    f"in {target_path.relative_to(REPO_ROOT)} declares it without a default. "
                    f"After Cadwyn pops the field, FastAPI re-validates the body against the "
                    f"HEAD response_model and fails. Make `{field_name}` Optional with a "
                    f"default (e.g. `{field_name}: T | None = None`)."
                )
                errors += 1
    return errors


def main() -> int:
    if not VERSIONS_DIR.is_dir():
        print(f"⚠️  versions directory not found: {VERSIONS_DIR}", file=sys.stderr)
        return 0
    errors = 0
    for version_file in sorted(VERSIONS_DIR.glob("v*.py")):
        errors += _check_file(version_file)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
