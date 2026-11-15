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
"""
Static import graph of the Airflow sources and tests, used to prove that a provider's tests cannot
reach any changed module.

Only the stdlib ``ast`` reads the files; nothing is imported or executed. The graph over-approximates:
imports at any level (except under ``if TYPE_CHECKING:``) and every string constant naming a module,
which covers ``mock.patch`` targets, ``pytest_plugins`` and lazy ``__getattr__`` tables, are edges.
Modules imported by a string from ``provider.yaml``, ``config.yml``, entry points or ``import_string``-like
calls with a literal argument can run in any test, so ``find_reachable_providers`` refuses to narrow a
change to them or to anything they import. Calls with a computed argument are not seen.
"""

from __future__ import annotations

import ast
import os
import re
import tomllib
from collections import defaultdict
from functools import cache
from pathlib import Path

import yaml

# Top-level provider.yaml lists that feed docs and static checks only; ProvidersManager imports
# nothing from them. Every other key (connection-types, triggers, extra-links, ...) may name a
# module that is imported by its dotted path at runtime.
PROVIDER_YAML_DOC_ONLY_KEYS = frozenset({"operators", "sensors", "hooks", "transfers"})
DYNAMIC_IMPORT_CALLS = frozenset({"import_string", "import_module", "getimport", "__import__"})
# Changes outside these trees cannot change what a provider's tests import.
TRACKED_PREFIXES = ("airflow-core/", "task-sdk/", "providers/", "devel-common/", "shared/")
# A changed file here may be loaded by any test: by the pytest plugin, by path, or through a symlink
# into several distributions.
NEVER_NARROWED_PREFIXES = ("devel-common/", "shared/")
# Files loaded by path (DagBag, example Dag tests, the test plugins folder, alembic), which no import
# edge records.
NEVER_NARROWED_DIRS = frozenset({"example_dags", "dags", "system", "plugins", "migrations"})
DOC_SUFFIXES = (".md", ".rst")
SKIPPED_DIRS = frozenset({"__pycache__", "node_modules", ".venv"})
DOTTED_NAME = re.compile(r"^[A-Za-z_]\w*(\.\w+)+$")


def _find_provider_dirs(root: Path) -> list[Path]:
    """Provider distributions; a ``provider.yaml`` or ``pyproject.toml`` under tests is a fixture."""
    return [
        provider_yaml.parent
        for provider_yaml in sorted((root / "providers").glob("**/provider.yaml"))
        if not SKIPPED_DIRS.intersection(provider_yaml.parts)
        and "tests" not in provider_yaml.relative_to(root).parts
        and (provider_yaml.parent / "pyproject.toml").is_file()
    ]


def _find_roots(root: Path) -> list[Path]:
    roots = [root / "airflow-core/src", root / "airflow-core/tests", root / "task-sdk/src"]
    roots += [root / "task-sdk/tests", root / "devel-common/src"]
    for provider_dir in _find_provider_dirs(root):
        roots += [provider_dir / "src", provider_dir / "tests"]
    return [r for r in roots if r.is_dir()]


def _index_modules(root: Path) -> tuple[dict[str, list[str]], dict[str, str]]:
    modules: dict[str, list[str]] = defaultdict(list)
    path_to_module: dict[str, str] = {}
    for source_root in _find_roots(root):
        # followlinks: airflow/_shared/<lib> are symlinks to shared/<lib>/src.
        for dirpath, dirnames, filenames in os.walk(source_root, followlinks=True):
            dirnames[:] = [d for d in dirnames if d not in SKIPPED_DIRS]
            for filename in filenames:
                if not filename.endswith(".py"):
                    continue
                path = Path(dirpath, filename)
                parts = path.relative_to(source_root).with_suffix("").parts
                name = ".".join(parts[:-1] if parts[-1] == "__init__" else parts)
                relative = path.relative_to(root).as_posix()
                if filename == "conftest.py":
                    # pytest loads each conftest by path; one shared name would merge every provider's.
                    name = relative
                modules[name].append(relative)
                path_to_module[relative] = name
    return dict(modules), path_to_module


def _resolve(dotted: str, modules: dict[str, list[str]]) -> list[str]:
    """Every module that importing or naming ``dotted`` runs: its existing package prefixes."""
    parts = dotted.replace(":", ".").split(".")
    return [m for i in range(1, len(parts) + 1) if (m := ".".join(parts[:i])) in modules]


def _resolve_imported_name(module: str, name: str, modules: dict[str, list[str]]) -> list[str]:
    """``from module import name``; ``*`` can load any child listed in ``__all__``, so take them all."""
    if name != "*":
        return _resolve(f"{module}.{name}", modules)
    prefix = f"{module}."
    return [m for m in modules if m.startswith(prefix) and "." not in m[len(prefix) :]]


def _parse(root: Path, relative: str) -> ast.Module | None:
    try:
        return ast.parse((root / relative).read_text(errors="replace"))
    except (OSError, SyntaxError, ValueError):
        return None


def _get_type_checking_nodes(tree: ast.Module) -> set[int]:
    return {
        id(child)
        for node in ast.walk(tree)
        if isinstance(node, ast.If) and ast.unparse(node.test) in ("TYPE_CHECKING", "typing.TYPE_CHECKING")
        for statement in node.body
        for child in ast.walk(statement)
    }


def _extract_edges(
    tree: ast.Module, name: str, is_package: bool, modules: dict[str, list[str]]
) -> tuple[set[str], set[str]]:
    """Return the modules ``tree`` imports or names, and those it passes to a dynamic import call."""
    package = name if is_package else name.rpartition(".")[0]
    skipped = _get_type_checking_nodes(tree)
    edges: set[str] = set()
    dynamic: set[str] = set()
    for node in ast.walk(tree):
        if id(node) in skipped:
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                edges.update(_resolve(alias.name, modules))
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = ".".join(package.split(".")[: len(package.split(".")) - node.level + 1])
                imported = f"{base}.{node.module}" if node.module else base
            else:
                imported = node.module or ""
            edges.update(_resolve(imported, modules))
            for alias in node.names:
                edges.update(_resolve_imported_name(imported, alias.name, modules))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
            if value.startswith(".") and DOTTED_NAME.match("x" + value):
                level = len(value) - len(value.lstrip("."))
                base = ".".join(package.split(".")[: len(package.split(".")) - level + 1])
                value = f"{base}.{value.lstrip('.')}"
            if DOTTED_NAME.match(value.replace(":", ".")):
                edges.update(_resolve(value, modules))
        elif isinstance(node, ast.Call):
            func = node.func
            called = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if called in DYNAMIC_IMPORT_CALLS:
                for imported, child in _extract_dynamic_import_names(node):
                    loaded = _resolve(imported, modules)
                    if child:
                        loaded += _resolve_imported_name(imported, child, modules)
                    edges.update(loaded)
                    dynamic.update(loaded)
    edges.discard(name)
    return edges, dynamic


def _get_literal(node: ast.expr | None) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _extract_dynamic_import_names(call: ast.Call) -> list[tuple[str, str | None]]:
    """The module, and each ``fromlist`` name, that a literal ``import_module`` or ``__import__`` loads."""
    keywords = {keyword.arg: keyword.value for keyword in call.keywords}
    name = _get_literal(call.args[0]) if call.args else _get_literal(keywords.get("name"))
    if name is None:
        return []
    package = _get_literal(keywords.get("package", call.args[1] if len(call.args) > 1 else None))
    if name.startswith(".") and package:
        level = len(name) - len(name.lstrip("."))
        parts = package.split(".")
        name = ".".join([*parts[: len(parts) - level + 1], name.lstrip(".")]).rstrip(".")
    fromlist = keywords.get("fromlist", call.args[3] if len(call.args) > 3 else None)
    children = fromlist.elts if isinstance(fromlist, (ast.List, ast.Tuple)) else []
    return [(name, None), *((name, child) for element in children if (child := _get_literal(element)))]


def _collect_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for v in value.values() for s in _collect_strings(v)]
    if isinstance(value, list):
        return [s for v in value for s in _collect_strings(v)]
    return []


def _find_declared_dynamic_modules(root: Path, modules: dict[str, list[str]]) -> set[str]:
    """Modules named in files that Airflow imports from by dotted path at runtime."""
    strings: list[str] = []
    provider_dirs = _find_provider_dirs(root)
    for provider_dir in provider_dirs:
        data = yaml.safe_load((provider_dir / "provider.yaml").read_text()) or {}
        strings += _collect_strings({k: v for k, v in data.items() if k not in PROVIDER_YAML_DOC_ONLY_KEYS})
    config_yml = root / "airflow-core/src/airflow/config_templates/config.yml"
    strings += _collect_strings(yaml.safe_load(config_yml.read_text()))
    for distribution_dir in [root, root / "airflow-core", root / "task-sdk", *provider_dirs]:
        pyproject = distribution_dir / "pyproject.toml"
        if not pyproject.is_file():
            continue
        project = tomllib.loads(pyproject.read_text()).get("project", {})
        strings += _collect_strings(project.get("entry-points", {}))
        strings += _collect_strings(project.get("scripts", {}))
    # Importing a module by its dotted path also runs every parent package's ``__init__``.
    return {
        module
        for s in strings
        if DOTTED_NAME.match(s.strip().replace(":", "."))
        for module in _resolve(s.strip(), modules)
    }


class ImportGraph:
    def __init__(self, root: Path):
        self.modules, self.path_to_module = _index_modules(root)
        self.edges: dict[str, set[str]] = defaultdict(set)
        self.dynamic_modules = _find_declared_dynamic_modules(root, self.modules)
        self._provider_closures: dict[str, set[str]] = {}
        for name, paths in self.modules.items():
            for relative in paths:
                if relative.endswith("/get_provider_info.py"):
                    # Generated mirror of provider.yaml, read through an entry point: covered by
                    # dynamic_modules, and its doc-only lists would make every module look imported.
                    continue
                tree = _parse(root, relative)
                if tree is None:
                    # Its imports are unknown, so a change to it cannot be narrowed.
                    self.dynamic_modules.add(name)
                    continue
                edges, dynamic = _extract_edges(tree, name, relative.endswith("/__init__.py"), self.modules)
                self.edges[name] |= edges
                self.dynamic_modules |= dynamic
        self.dynamic_closure = self.find_closure(self.dynamic_modules)

    def find_closure(self, start: set[str]) -> set[str]:
        seen = set(start)
        pending = list(start)
        while pending:
            for dependency in self.edges.get(pending.pop(), ()):
                if dependency not in seen:
                    seen.add(dependency)
                    pending.append(dependency)
        return seen

    def find_provider_test_closure(self, provider_id: str) -> set[str]:
        """Every module the tests of a provider can load."""
        if provider_id not in self._provider_closures:
            tests_prefix = f"providers/{provider_id.replace('.', '/')}/tests/"
            start = {m for p, m in self.path_to_module.items() if p.startswith(tests_prefix)}
            self._provider_closures[provider_id] = self.find_closure(start)
        return self._provider_closures[provider_id]


@cache
def get_import_graph(root: Path) -> ImportGraph:
    return ImportGraph(root)


def find_reachable_providers(
    root: Path, changed_files: tuple[str, ...], candidate_providers: set[str]
) -> set[str] | None:
    """
    Return the candidate providers whose tests can reach a changed file.

    ``None`` means the graph cannot prove anything for this change, and the caller must keep the
    coarse selection: a changed file is not a plain Python module, may be imported by a dotted path
    string or by a module that is, is a conftest, or is loaded by every test or by file path. Changed
    files outside ``TRACKED_PREFIXES`` are ignored.
    """
    graph = get_import_graph(root)
    changed_modules: set[str] = set()
    for changed in changed_files:
        if not changed.startswith(TRACKED_PREFIXES):
            continue
        if changed.startswith(NEVER_NARROWED_PREFIXES):
            return None
        module = graph.path_to_module.get(changed)
        if module is None and (
            "/docs/" in changed or "/newsfragments/" in changed or changed.endswith(DOC_SUFFIXES)
        ):
            continue
        if (
            module is None
            or module in graph.dynamic_closure
            or Path(changed).name == "conftest.py"
            or NEVER_NARROWED_DIRS.intersection(Path(changed).parts)
        ):
            return None
        changed_modules.add(module)
    return {p for p in candidate_providers if graph.find_provider_test_closure(p) & changed_modules}
