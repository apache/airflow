#!/usr/bin/env python
#
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
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
"""Check all provider-registered task decorators have synchronized typing declarations.

``provider.yaml`` is the only input. Each registered factory is followed through the source without
importing it:

1. The factory's call passing ``decorated_operator_class=`` names the wrapper operator class.
2. Base classes are resolved through imports, including compat modules that re-export them.
3. The wrapper's method resolution order is built with Python's C3 rule and cut at ``BaseOperator``,
   whose ``__init__`` forwards nothing to ``super()`` and rejects leftover keyword arguments.
4. Each constructor before the cut contributes its parameters. Keywords that a factory or constructor
   fixes on its forwarding call, such as ``cmds=None``, or always overwrites in ``**kwargs``, such as
   ``kwargs["serialize_output"] = True``, cannot be set by users and are excluded.

BaseOperator arguments, annotations, and defaults are outside this check's scope.
"""

from __future__ import annotations

import ast
import builtins
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import yaml
from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    AIRFLOW_TASK_SDK_SOURCES_PATH,
    console,
)


@dataclass(frozen=True)
class Definition:
    """Identify a top-level class or function by repository-relative module path and name."""

    path: Path
    name: str

    def __str__(self) -> str:
        return f"{self.path}:{self.name}"


@dataclass(frozen=True)
class Signature:
    """Map accepted and internally supplied parameter names to the source line defining them."""

    wrapper: Definition
    accepted: dict[str, str]
    supplied: dict[str, str]


SDK_SOURCES = AIRFLOW_TASK_SDK_SOURCES_PATH.relative_to(AIRFLOW_ROOT_PATH)
CORE_SOURCES = AIRFLOW_CORE_SOURCES_PATH.relative_to(AIRFLOW_ROOT_PATH)
SDK_PACKAGE = SDK_SOURCES / "airflow/sdk"
STUB_PATH = SDK_PACKAGE / "definitions/decorators/__init__.pyi"
COLLECTION_PATH = STUB_PATH.with_suffix(".py")
PROVIDERS_PATH = AIRFLOW_PROVIDERS_ROOT_PATH.relative_to(AIRFLOW_ROOT_PATH)
BASE_OPERATOR = Definition(SDK_PACKAGE / "bases/operator.py", "BaseOperator")
# Skipped: its parameters are framework-supplied or, like task_id, BaseOperator arguments.
DECORATED_OPERATOR = Definition(SDK_PACKAGE / "bases/decorator.py", "DecoratedOperator")
FRAMEWORK_PARAMETERS = frozenset({"python_callable", "op_args", "op_kwargs", "kwargs_to_upstream"})


def extract_parameter_names(function: ast.FunctionDef) -> set[str]:
    """Extract explicit parameter names, excluding self, cls, and variadic arguments."""
    args = function.args
    return {arg.arg for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs)} - {"self", "cls"}


def parse_source(root: Path, path: Path) -> ast.Module:
    """Parse a source file, reporting read and syntax errors with its repository-relative location."""
    try:
        return ast.parse((root / path).read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as error:
        raise ValueError(f"{path}:{error.lineno}: {error.msg}") from error
    except OSError as error:
        raise ValueError(f"{path}: {error.strerror}") from error
    except UnicodeError as error:
        raise ValueError(f"{path}: {error}") from error


def get_class_body(root: Path, path: Path, class_name: str) -> list[ast.stmt]:
    """Read a uniquely named top-level class without importing its module."""
    tree = parse_source(root, path)
    classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name]
    if len(classes) != 1:
        raise ValueError(f"{path}: expected one class {class_name}, found {len(classes)}")
    return classes[0].body


def iter_module_statements(body: Iterable[ast.stmt]) -> Iterator[ast.stmt]:
    """Yield module-level statements in source order, including those nested in ``if`` and ``try``."""
    for node in body:
        if isinstance(node, ast.If):
            yield from iter_module_statements(node.body)
            yield from iter_module_statements(node.orelse)
        elif isinstance(node, (ast.Try, ast.TryStar)):
            yield from iter_module_statements(node.body)
            for handler in node.handlers:
                yield from iter_module_statements(handler.body)
            yield from iter_module_statements(node.orelse)
            yield from iter_module_statements(node.finalbody)
        else:
            yield node


def find_lazy_import_modules(statements: Iterable[ast.stmt], name: str) -> list[str]:
    """Find modules a lazy ``__getattr__`` table such as ``{"Name": ("module", ...)}`` lists for a name."""
    modules: list[str] = []
    for node in statements:
        table = node.value if isinstance(node, (ast.Assign, ast.AnnAssign)) else None
        if not isinstance(table, ast.Dict):
            continue
        for key, value in zip(table.keys, table.values):
            if isinstance(key, ast.Constant) and key.value == name:
                entries = value.elts if isinstance(value, ast.Tuple) else [value]
                modules.extend(
                    entry.value
                    for entry in entries
                    if isinstance(entry, ast.Constant) and isinstance(entry.value, str)
                )
    return modules


def has_name_reference(expression: ast.expr, names: set[str]) -> bool:
    """Check whether an expression reads any of the given variable names."""
    return any(isinstance(node, ast.Name) and node.id in names for node in ast.walk(expression))


def find_user_values(function: ast.FunctionDef) -> set[str]:
    """Find variables holding caller input: parameters, ``*args``/``**kwargs``, and locals computed from them."""
    user_values = extract_parameter_names(function) | {
        arg.arg for arg in (function.args.vararg, function.args.kwarg) if arg
    }
    assignments = [
        (node.targets if isinstance(node, ast.Assign) else [node.target], node.value)
        for node in ast.walk(function)
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None
    ]
    changed = True
    while changed:
        changed = False
        for targets, value in assignments:
            names = {target.id for target in targets if isinstance(target, ast.Name)}
            if not names <= user_values and has_name_reference(value, user_values):
                user_values |= names
                changed = True
    return user_values


def find_internally_supplied(function: ast.FunctionDef, call: ast.Call) -> set[str]:
    """Find keywords a function fixes on a forwarding call, so users cannot pass them via ``**kwargs``.

    A keyword stays user-settable when it is a parameter of the function or its value is computed from
    one or from ``**kwargs``, as in ``name=kwargs.pop("name", default)``. Only the value counts:
    ``cmds = kwargs.pop("cmds")`` followed by ``cmds=None`` still supplies ``cmds`` internally.
    """
    parameters = extract_parameter_names(function)
    user_values = find_user_values(function)
    return {
        keyword.arg
        for keyword in call.keywords
        if keyword.arg is not None
        and keyword.arg not in parameters
        and not has_name_reference(keyword.value, user_values)
    }


def find_overridden_kwargs(function: ast.FunctionDef) -> set[str]:
    """Find ``**kwargs`` keys a function always overwrites with a fixed value before forwarding them.

    Only statements directly in the function body count: ``kwargs["key"] = 5`` under ``if "key" not in
    kwargs`` sets a default that users can still override.
    """
    if function.args.kwarg is None:
        return set()
    kwargs_name = function.args.kwarg.arg
    user_values = find_user_values(function)
    return {
        target.slice.value
        for statement in function.body
        if isinstance(statement, ast.Assign) and not has_name_reference(statement.value, user_values)
        for target in statement.targets
        if isinstance(target, ast.Subscript)
        and isinstance(target.value, ast.Name)
        and target.value.id == kwargs_name
        and isinstance(target.slice, ast.Constant)
        and isinstance(target.slice.value, str)
    }


def find_constructor_calls(function: ast.FunctionDef) -> list[ast.Call]:
    """Find ``super().__init__(...)`` and explicit ``Parent.__init__(self, ...)`` calls."""
    return [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "__init__"
    ]


def merge_linearizations(sequences: list[list[Definition]], owner: Definition) -> list[Definition]:
    """Merge base class orders with C3: repeatedly take the first head no sequence lists later."""
    remaining = [sequence for sequence in sequences if sequence]
    merged = []
    while remaining:
        head = next(
            (
                sequence[0]
                for sequence in remaining
                if not any(sequence[0] in other[1:] for other in remaining)
            ),
            None,
        )
        if head is None:
            raise ValueError(f"{owner}: cannot create a consistent method resolution order")
        merged.append(head)
        remaining = [
            rest for sequence in remaining if (rest := sequence[1:] if sequence[0] == head else sequence)
        ]
    return merged


def find_provider_metadata(root: Path) -> list[Path]:
    """Find provider metadata files, excluding the linked provider source tree."""
    providers_root = root / PROVIDERS_PATH
    return sorted(
        path
        for path in providers_root.rglob("provider.yaml")
        if not path.is_relative_to(providers_root / "src")
    )


def discover_decorators(root: Path) -> tuple[dict[str, Definition], list[str]]:
    """Read every provider's task registrations and report invalid or duplicate entries."""
    registrations: dict[str, Definition] = {}
    errors = []
    metadata_files = find_provider_metadata(root)
    if not metadata_files:
        return {}, [f"{PROVIDERS_PATH}: no provider.yaml files found"]
    for path in metadata_files:
        relative_path = path.relative_to(root)
        try:
            metadata = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(metadata, dict):
                raise ValueError("expected a provider metadata mapping")
            entries = metadata.get("task-decorators", [])
            if not isinstance(entries, list):
                raise ValueError("task-decorators must be a list")
            for entry in entries:
                if not isinstance(entry, dict):
                    raise ValueError("task-decorators entries must be mappings")
                name, factory = entry.get("name"), entry.get("class-name")
                if not isinstance(name, str) or not name.isidentifier():
                    raise ValueError(f"invalid task decorator name: {name!r}")
                if (
                    not isinstance(factory, str)
                    or "." not in factory
                    or not all(part.isidentifier() for part in factory.split("."))
                ):
                    raise ValueError(f"@task.{name}: invalid class-name: {factory!r}")
                if name in registrations:
                    raise ValueError(f"@task.{name}: duplicate registration")
                module, function = factory.rsplit(".", 1)
                registrations[name] = Definition(
                    relative_path.parent / "src" / Path(*module.split(".")).with_suffix(".py"), function
                )
        except (OSError, UnicodeError, yaml.YAMLError, ValueError) as error:
            errors.append(f"{relative_path}: {error}")
    return registrations, errors


class SourceTree:
    """Find definitions in repository sources by following imports, without importing anything."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.source_roots = [
            SDK_SOURCES,
            CORE_SOURCES,
            *(path.parent.relative_to(root) / "src" for path in find_provider_metadata(root)),
        ]
        self._modules: dict[Path, ast.Module] = {}
        self._module_paths: dict[str, Path | None] = {}
        self._resolved: dict[tuple[Path, str], Definition | None] = {}
        self._linearized: dict[Definition, list[Definition]] = {}

    def parse(self, path: Path) -> ast.Module:
        """Parse a repository-relative source file once."""
        if path not in self._modules:
            self._modules[path] = parse_source(self.root, path)
        return self._modules[path]

    def find_module(self, module: str) -> Path | None:
        """Map a dotted module name to its source file, or None when it is not in the repository."""
        if module not in self._module_paths:
            parts = module.split(".")
            candidates = (
                base
                for source_root in self.source_roots
                for base in (
                    source_root.joinpath(*parts).with_suffix(".py"),
                    source_root.joinpath(*parts, "__init__.py"),
                )
            )
            self._module_paths[module] = next((c for c in candidates if (self.root / c).is_file()), None)
        return self._module_paths[module]

    def get_absolute_module(self, path: Path, module: str | None, level: int) -> str:
        """Turn a possibly relative import in the module at ``path`` into a dotted module name."""
        if not level:
            return module or ""
        source_root = next((r for r in self.source_roots if path.is_relative_to(r)), None)
        if source_root is None:
            raise ValueError(f"{path}: relative import outside a known source root")
        package = list(path.relative_to(source_root).parent.parts)
        return ".".join([*package[: len(package) - level + 1], *([module] if module else [])])

    def resolve(self, path: Path, name: str) -> Definition | None:
        """Find where a name used in the module at ``path`` is defined, or None if not in the repository.

        The first binding in source order that resolves wins, which picks the ``TYPE_CHECKING`` or
        newest-version branch of compatibility imports. Lazy ``__getattr__`` tables are tried last.
        """
        key = (path, name)
        if key not in self._resolved:
            # Recorded before searching so that modules re-exporting from each other terminate.
            self._resolved[key] = None
            try:
                self._resolved[key] = self._find_definition(path, name)
            except ValueError:
                # Unreadable sources must keep failing for every decorator that reaches them.
                del self._resolved[key]
                raise
        return self._resolved[key]

    def _find_definition(self, path: Path, name: str) -> Definition | None:
        statements = list(iter_module_statements(self.parse(path).body))
        for node in statements:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name == name:
                return Definition(path, name)
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if (alias.asname or alias.name) == name:
                        module = self.get_absolute_module(path, node.module, node.level)
                        if found := self._resolve_in_module(module, alias.name):
                            return found
        for module in find_lazy_import_modules(statements, name):
            stripped = module.lstrip(".")
            absolute = self.get_absolute_module(path, stripped or None, len(module) - len(stripped))
            if found := self._resolve_in_module(absolute, name):
                return found
        return None

    def _resolve_in_module(self, module: str, name: str) -> Definition | None:
        module_path = self.find_module(module)
        return self.resolve(module_path, name) if module_path else None

    def is_external(self, path: Path, name: str) -> bool:
        """Check whether a name is a builtin or imported from a package other than ``airflow``."""
        for node in iter_module_statements(self.parse(path).body):
            if isinstance(node, ast.ImportFrom) and not node.level and node.module:
                if any((alias.asname or alias.name) == name for alias in node.names):
                    return node.module.split(".")[0] != "airflow"
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if (alias.asname or alias.name.split(".")[0]) == name:
                        return alias.name.split(".")[0] != "airflow"
        return name in vars(builtins)

    def get_node(self, definition: Definition) -> ast.ClassDef | ast.FunctionDef:
        """Return the first top-level class or function definition with the given name."""
        for node in iter_module_statements(self.parse(definition.path).body):
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name == definition.name:
                return node
        raise ValueError(f"{definition.path}: no top-level class or function {definition.name}")

    def get_function(self, definition: Definition) -> ast.FunctionDef:
        """Return a top-level function definition."""
        node = self.get_node(definition)
        if not isinstance(node, ast.FunctionDef):
            raise ValueError(f"{definition}: expected a function")
        return node

    def get_constructor(self, definition: Definition) -> ast.FunctionDef | None:
        """Return the class's own ``__init__``, or None when it inherits one."""
        body = self.get_node(definition).body
        constructors = [n for n in body if isinstance(n, ast.FunctionDef) and n.name == "__init__"]
        if len(constructors) > 1:
            raise ValueError(f"{definition}: expected at most one __init__, found {len(constructors)}")
        return constructors[0] if constructors else None

    def resolve_expression(self, path: Path, expression: ast.expr, role: str) -> Definition:
        """Resolve a plain name used in a module, failing when it is not defined in the repository."""
        found = self.resolve(path, expression.id) if isinstance(expression, ast.Name) else None
        if found is None:
            raise ValueError(f"{path}:{expression.lineno}: cannot resolve {role} {ast.unparse(expression)}")
        return found

    def get_bases(self, definition: Definition) -> list[Definition]:
        """Resolve a class's direct bases, skipping builtins and classes from outside ``airflow``."""
        if definition == BASE_OPERATOR:
            return []
        node = self.get_node(definition)
        if not isinstance(node, ast.ClassDef):
            raise ValueError(f"{definition}: expected a class")
        bases = []
        for expression in node.bases:
            target = expression.value if isinstance(expression, ast.Subscript) else expression
            leftmost = target
            while isinstance(leftmost, ast.Attribute):
                leftmost = leftmost.value
            if isinstance(leftmost, ast.Name) and self.is_external(definition.path, leftmost.id):
                continue
            if not isinstance(target, ast.Name):
                raise ValueError(
                    f"{definition.path}:{expression.lineno}: unsupported base class {ast.unparse(expression)}"
                )
            bases.append(self.resolve_expression(definition.path, target, "base class"))
        return bases

    def linearize(self, definition: Definition) -> list[Definition]:
        """Order a class and its ancestors the way Python's C3 algorithm builds ``__mro__``."""
        if definition not in self._linearized:
            bases = self.get_bases(definition)
            sequences = [*(self.linearize(base) for base in bases), bases]
            self._linearized[definition] = [definition, *merge_linearizations(sequences, definition)]
        return self._linearized[definition]


def find_operator_class_call(factory: Definition, function: ast.FunctionDef) -> ast.Call:
    """Find the factory's single call passing ``decorated_operator_class``."""
    calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and any(keyword.arg == "decorated_operator_class" for keyword in node.keywords)
    ]
    if len(calls) != 1:
        raise ValueError(
            f"{factory.path}:{function.lineno}: expected {factory.name} to make one call passing "
            f"decorated_operator_class, found {len(calls)}"
        )
    return calls[0]


def add_parameters(accepted: dict[str, str], path: Path, function: ast.FunctionDef) -> None:
    """Record a function's parameters, keeping the first location found for each name."""
    for name in extract_parameter_names(function):
        accepted.setdefault(name, f"{path}:{function.lineno}")


def derive_signature(tree: SourceTree, factory: Definition) -> Signature:
    """Follow a registered factory to the parameters its decorator accepts."""
    function = tree.get_function(factory)
    call = find_operator_class_call(factory, function)
    callee = tree.resolve_expression(factory.path, call.func, "factory call")
    wrapper_expression = next(k.value for k in call.keywords if k.arg == "decorated_operator_class")
    wrapper = tree.resolve_expression(factory.path, wrapper_expression, "decorated_operator_class")

    accepted: dict[str, str] = {}
    add_parameters(accepted, factory.path, function)
    add_parameters(accepted, callee.path, tree.get_function(callee))
    location = f"{factory.path}:{function.lineno}"
    supplied = dict.fromkeys(find_internally_supplied(function, call), location)

    order = tree.linearize(wrapper)
    if BASE_OPERATOR not in order:
        raise ValueError(f"{wrapper}: does not inherit {BASE_OPERATOR}")
    declared_earlier: set[str] = set()
    for definition in order[: order.index(BASE_OPERATOR)]:
        constructor = None if definition == DECORATED_OPERATOR else tree.get_constructor(definition)
        if constructor is None:
            continue
        location = f"{definition.path}:{constructor.lineno}"
        fixed = find_overridden_kwargs(constructor).union(
            *(find_internally_supplied(constructor, c) for c in find_constructor_calls(constructor))
        )
        for name in fixed - declared_earlier:
            supplied.setdefault(name, location)
        declared_earlier |= extract_parameter_names(constructor)
        add_parameters(accepted, definition.path, constructor)

    for name in supplied.keys() | FRAMEWORK_PARAMETERS:
        accepted.pop(name, None)
    return Signature(wrapper, accepted, supplied)


def extract_collection_members(root: Path, path: Path) -> set[str]:
    """Find named methods and attributes, including SDK-owned staticmethod assignments."""
    names = set()
    for node in get_class_body(root, path, "TaskDecoratorCollection"):
        if isinstance(node, ast.FunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def get_stub_declaration(root: Path, name: str) -> ast.FunctionDef:
    """Find the stub overload configuring a decorator, ignoring overloads accepting ``python_callable``."""
    overloads = [
        node
        for node in get_class_body(root, STUB_PATH, "TaskDecoratorCollection")
        if isinstance(node, ast.FunctionDef)
        and node.name == name
        and "python_callable" not in extract_parameter_names(node)
    ]
    if len(overloads) != 1:
        raise ValueError(
            f"{STUB_PATH}: expected one configuration overload TaskDecoratorCollection.{name}, "
            f"found {len(overloads)}"
        )
    return overloads[0]


def collect_coverage_errors(root: Path, registrations: dict[str, Definition]) -> list[str]:
    """Require provider registrations and named typing declarations to agree."""
    try:
        builtin_members = extract_collection_members(root, COLLECTION_PATH)
        declared = extract_collection_members(root, STUB_PATH)
    except ValueError as error:
        return [str(error)]
    errors = []
    for name in sorted(registrations.keys() & builtin_members):
        errors.append(f"@task.{name}: provider registration conflicts with an SDK-owned collection member")
    for name in sorted(registrations.keys() - declared):
        errors.append(f"@task.{name}: registered without a named typing declaration. Update {STUB_PATH}.")
    for name in sorted(declared - builtin_members - registrations.keys()):
        errors.append(f"@task.{name}: typing declaration has no provider registration. Update {STUB_PATH}.")
    return errors


def collect_argument_errors(root: Path, registrations: dict[str, Definition]) -> list[str]:
    """Report parameter mismatches between each registered decorator and its typing declaration."""
    tree = SourceTree(root)
    errors = []
    for name, factory in sorted(registrations.items()):
        try:
            signature = derive_signature(tree, factory)
            stub = get_stub_declaration(root, name)
        except ValueError as error:
            errors.append(f"@task.{name}: {error}")
            continue

        declared = extract_parameter_names(stub)
        location = f"{STUB_PATH}:{stub.lineno}: @task.{name}"
        missing = signature.accepted.keys() - declared
        if missing:
            sources = ", ".join(sorted({signature.accepted[parameter] for parameter in missing}))
            errors.append(
                f"{location} is missing parameters: {', '.join(sorted(missing))}. "
                f"Declared in {sources}. Update {STUB_PATH}."
            )
        unexpected = declared - signature.accepted.keys()
        if unexpected:
            supplied = "".join(
                f" {parameter} is internally supplied at {signature.supplied[parameter]}."
                for parameter in sorted(unexpected & signature.supplied.keys())
            )
            errors.append(
                f"{location} declares unexpected parameters: {', '.join(sorted(unexpected))}.{supplied} "
                f"Update {STUB_PATH} to match {signature.wrapper} and the constructors it inherits."
            )
        if name == "python":
            try:
                alias = get_stub_declaration(root, "__call__")
                if extract_parameter_names(alias) != declared:
                    errors.append(
                        f"{STUB_PATH}:{alias.lineno}: @task.__call__ parameters differ from @task.python. "
                        f"Update both declarations in {STUB_PATH}."
                    )
            except ValueError as error:
                errors.append(f"@task.__call__: {error}")
    return errors


def collect_errors(root: Path = AIRFLOW_ROOT_PATH) -> list[str]:
    """Report inventory gaps and parameter drift for every registered task decorator."""
    registrations, errors = discover_decorators(root)
    return [
        *errors,
        *collect_coverage_errors(root, registrations),
        *collect_argument_errors(root, registrations),
    ]


def main(root: Path = AIRFLOW_ROOT_PATH) -> int:
    """Print check failures and return 1 on failure or 0 on success."""
    errors = collect_errors(root)
    for error in errors:
        console.print(error, markup=False)
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())
