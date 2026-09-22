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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check that every ``*conn_id`` argument an operator, sensor or notifier accepts is templated.

Connection ids are rendered like any other template field, so a user can pass
``conn_id="{{ params.conn_id }}"``. A class must therefore list each ``*conn_id`` argument it
accepts — directly or through a parent ``__init__`` — in the ``template_fields`` that Python
resolves for it. Because ``template_fields`` is a plain class attribute, a subclass that
redefines it without spreading the parent's fields silently drops the parent's connection ids,
which is why the whole inheritance chain is resolved instead of the class body alone.

An argument is not required to be templated when templating it cannot work: it is read in
``__init__`` (the value is still the raw Jinja string there, see ``validate_operators_init``),
or it is never stored under ``self.<argument>`` (there is nothing for the renderer to replace).
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, console
from validate_operators_init import _collect_sanctioned_uses, _resolve_base_name

CONN_ID_SUFFIX = "conn_id"
OPERATOR_SUFFIXES = ("Operator", "Sensor", "Notifier")
NOT_OPERATOR_SUFFIXES = ("Trigger", "Hook", "Link", "Mixin")
# Helper callables used as template_fields values, mapped to the fields they inject.
TEMPLATE_FIELD_HELPERS: dict[str, frozenset[str]] = {
    "aws_template_fields": frozenset({"aws_conn_id", "region_name", "verify"}),
}


@dataclass
class ClassInfo:
    name: str
    path: Path
    node: ast.ClassDef
    bases: list[str] = field(init=False)
    init: ast.FunctionDef | None = field(init=False)
    template_fields: ast.expr | None = field(init=False)

    def __post_init__(self) -> None:
        self.bases = [_resolve_base_name(base) for base in self.node.bases]
        self.init = next(
            (
                item
                for item in self.node.body
                if isinstance(item, ast.FunctionDef) and item.name == "__init__"
            ),
            None,
        )
        self.template_fields = None
        for item in self.node.body:
            target: ast.expr
            value: ast.expr | None
            if isinstance(item, ast.Assign):
                target, value = item.targets[0], item.value
            elif isinstance(item, ast.AnnAssign):
                target, value = item.target, item.value
            else:
                continue
            if isinstance(target, ast.Name) and target.id == "template_fields" and value is not None:
                self.template_fields = value


def iter_source_files(roots: list[Path]):
    """Yield source (non-test) Python files under the roots."""
    for root in roots:
        for path in root.rglob("*.py"):
            parts = path.relative_to(root).parts
            if "tests" not in parts:
                yield path


def parse_classes(path: Path) -> list[ClassInfo]:
    try:
        tree = ast.parse(path.read_text())
    except (SyntaxError, UnicodeDecodeError):
        return []
    return [ClassInfo(node.name, path, node) for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]


def build_index(roots: list[Path]) -> dict[str, ClassInfo]:
    """Index every class in the roots by name (first definition wins) for base-class resolution."""
    index: dict[str, ClassInfo] = {}
    for path in iter_source_files(roots):
        for info in parse_classes(path):
            index.setdefault(info.name, info)
    return index


def iter_ancestors(cls: ClassInfo, index: dict[str, ClassInfo], seen: set[str] | None = None):
    """Yield cls and its indexed ancestors depth-first, left to right, each once."""
    seen = set() if seen is None else seen
    if cls.name in seen:
        return
    seen.add(cls.name)
    yield cls
    for base in cls.bases:
        if base in index:
            yield from iter_ancestors(index[base], index, seen)


def is_operator(cls: ClassInfo, index: dict[str, ClassInfo]) -> bool:
    if cls.name.endswith(NOT_OPERATOR_SUFFIXES):
        return False
    for ancestor in iter_ancestors(cls, index):
        if ancestor.name.endswith(OPERATOR_SUFFIXES) or any(
            b.endswith(OPERATOR_SUFFIXES) for b in ancestor.bases
        ):
            return True
    return False


def resolve_template_fields(cls: ClassInfo, index: dict[str, ClassInfo], seen: frozenset[str] = frozenset()):
    """
    Return the template_fields Python resolves for cls, or None if no class in the chain defines them.

    Handles literal tuples/lists/sets, ``*Parent.template_fields`` spreads, ``Parent.template_fields``
    references inside ``tuple(set(...) | set(...))`` expressions, and known helper calls.
    """
    if cls.name in seen:
        return None
    seen = seen | {cls.name}
    if cls.template_fields is not None:
        fields: set[str] = set()
        for node in ast.walk(cls.template_fields):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                fields.add(node.value)
            elif isinstance(node, ast.Attribute) and node.attr == "template_fields":
                owner = _resolve_base_name(node.value)
                if owner in index:
                    fields |= resolve_template_fields(index[owner], index, seen) or set()
            elif isinstance(node, ast.Call):
                fields |= TEMPLATE_FIELD_HELPERS.get(_resolve_base_name(node.func), frozenset())
        return fields
    for base in cls.bases:
        if base in index:
            resolved = resolve_template_fields(index[base], index, seen)
            if resolved is not None:
                return resolved
    return None


def conn_id_params(init: ast.FunctionDef) -> list[str]:
    args = init.args
    return [
        a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs] if a.arg.endswith(CONN_ID_SUFFIX)
    ]


def is_read_in_init(init: ast.FunctionDef, param: str) -> bool:
    """True if __init__ uses the argument beyond storing it or forwarding it to the parent."""
    sanctioned = _collect_sanctioned_uses(init, [param])
    for node in ast.walk(init):
        if id(node) in sanctioned or not isinstance(node.ctx if hasattr(node, "ctx") else None, ast.Load):
            continue
        if isinstance(node, ast.Name) and node.id == param:
            return True
        if isinstance(node, ast.Attribute) and node.attr == param and isinstance(node.value, ast.Name):
            if node.value.id == "self":
                return True
    return False


def stores_attribute(cls: ClassInfo, attr: str, index: dict[str, ClassInfo]) -> bool:
    """True if cls or an ancestor assigns ``self.<attr>`` in its __init__."""
    for ancestor in iter_ancestors(cls, index):
        if ancestor.init is None:
            continue
        for node in ast.walk(ancestor.init):
            targets = node.targets if isinstance(node, ast.Assign) else [getattr(node, "target", None)]
            for target in targets:
                if isinstance(target, ast.Attribute) and target.attr == attr:
                    if isinstance(target.value, ast.Name) and target.value.id == "self":
                        return True
    return False


def required_conn_ids(cls: ClassInfo, index: dict[str, ClassInfo]) -> set[str]:
    """Connection-id arguments accepted anywhere in the chain that templating can act on."""
    chain = [ancestor for ancestor in iter_ancestors(cls, index) if ancestor.init is not None]
    declared = {param for ancestor in chain for param in conn_id_params(ancestor.init)}
    return {
        param
        for param in declared
        if stores_attribute(cls, param, index)
        and not any(is_read_in_init(ancestor.init, param) for ancestor in chain)
    }


def check_file(path: Path, index: dict[str, ClassInfo]) -> list[str]:
    errors = []
    for cls in parse_classes(path):
        if not is_operator(cls, index):
            continue
        missing = required_conn_ids(cls, index) - (resolve_template_fields(cls, index) or set())
        if missing:
            errors.append(
                f"{path}:{cls.node.lineno}: {cls.name}: {sorted(missing)} missing from template_fields"
            )
    return errors


def main(files: list[str], roots: list[Path]) -> int:
    candidates = [Path(f) for f in files if CONN_ID_SUFFIX in Path(f).read_text()]
    if not candidates:
        return 0
    index = build_index(roots)
    errors = [error for path in candidates for error in check_file(path, index)]
    for error in errors:
        console.print(f"[red]{error}[/]")
    if errors:
        console.print(
            "\n[yellow]Add the connection id to the class's template_fields (spreading the parent's "
            "fields if it redefines them), or read it only in execute().[/]"
        )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], [AIRFLOW_PROVIDERS_ROOT_PATH]))
