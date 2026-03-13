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
"""Module for provider's custom Sphinx extensions that will be loaded in all providers' documentation."""

from __future__ import annotations

import ast
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

# No stub exists for docutils.parsers.rst.directives. See https://github.com/python/typeshed/issues/5755.
from provider_yaml_utils import load_package_data

from sphinx_exts.operators_and_hooks_ref import (
    DEFAULT_HEADER_SEPARATOR,
    BaseJinjaReferenceDirective,
    _render_template,
)


def find_class_methods_with_specific_calls(
    class_node: ast.ClassDef, target_calls: set[str], import_mappings: dict[str, str]
) -> set[str]:
    """
    Identifies class methods that make specific calls.

    This function only tracks target calls within the class scope. Method calling some function defined
    will not be taken into consideration even if this function performs a target call.

    Method calling other method that performs a target call will also be included.

    This function performs a two-pass analysis of the AST:
    1. It first identifies methods containing direct calls to the specified functions
       and records method calls on `self`.
    2. It then identifies methods that indirectly make such calls by invoking the
       methods identified in the first pass.

    :param class_node: The root node of the AST representing the class to analyze.
    :param target_calls: A set of full paths to the method names to track when called.
    :param import_mappings: A mapping of import names to fully qualified module names.

    :return: Method names within the class that either directly or indirectly make the specified calls.

    Examples:
        > source_code = '''
        ... class Example:
        ...     def method1(self):
        ...         my_method().ok()

        ...     def method2(self):
        ...         self.method1()

        ...     def method3(self):
        ...         my_method().not_ok()

        ...     def method4(self):
        ...         self.some_other_method()

        ...     def method5(self):
        ...         direct_call()
        ... '''
        > find_methods_with_specific_calls(
            ast.parse(source_code),
            {"airflow.my_method.not_ok", "airflow.my_method.ok", "airflow.direct_call"},
            {"my_method": "airflow.my_method", "direct_call": "airflow.direct_call"}
        )
        {'method1', 'method2', 'method3', 'method5'}
    """
    method_call_map: dict[str, set[str]] = {}
    methods_with_calls: set[str] = set()

    # First pass: Collect all calls and identify methods with specific calls we are looking for
    for node in ast.walk(class_node):
        if not isinstance(node, ast.FunctionDef):
            continue
        method_call_map[node.name] = set()
        for sub_node in ast.walk(node):
            if not isinstance(sub_node, ast.Call):
                continue
            called_function = sub_node.func
            # Direct function calls: e.g. send_sql_hook_lineage(...)
            if isinstance(called_function, ast.Name):
                full_call = import_mappings.get(called_function.id)
                if full_call in target_calls:
                    methods_with_calls.add(node.name)
                continue
            if not isinstance(called_function, ast.Attribute):
                continue
            if isinstance(called_function.value, ast.Call) and isinstance(
                called_function.value.func, ast.Name
            ):
                full_method_call = (
                    f"{import_mappings.get(called_function.value.func.id)}.{called_function.attr}"
                )
                if full_method_call in target_calls:
                    methods_with_calls.add(node.name)
            elif isinstance(called_function.value, ast.Name) and called_function.value.id == "self":
                method_call_map[node.name].add(called_function.attr)

    # Second pass: Identify all methods that call the ones in `methods_with_calls`
    def find_calling_methods(method_name):
        for caller, callees in method_call_map.items():
            if method_name in callees and caller not in methods_with_calls:
                methods_with_calls.add(caller)
                find_calling_methods(caller)

    for method in list(methods_with_calls):
        find_calling_methods(method)

    return methods_with_calls


def get_import_mappings(tree) -> dict[str, str]:
    """
    Retrieve a mapping of local import names to their fully qualified module paths from an AST tree.

    :param tree: The AST tree to analyze for import statements.

    :return: A dictionary where the keys are the local names (aliases) used in the current module
        and the values are the fully qualified names of the imported modules or their members.

    Example:
        >>> import ast
        >>> code = '''
        ... import os
        ... import numpy as np
        ... from collections import defaultdict
        ... from datetime import datetime as dt
        ... '''
        >>> get_import_mappings(ast.parse(code))
        {'os': 'os', 'np': 'numpy', 'defaultdict': 'collections.defaultdict', 'dt': 'datetime.datetime'}
    """
    imports = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                module_prefix = f"{node.module}." if hasattr(node, "module") and node.module else ""
                imports[alias.asname or alias.name] = f"{module_prefix}{alias.name}"
    return imports


def _get_module_class_registry(
    module_filepath: Path, module_name: str, class_extras: dict[str, Callable]
) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]]]:
    """
    Extracts classes and module-level functions from a Python module file.

    The function parses the specified module file and registers all classes.
    The registry for each class includes the module filename, methods, base classes,
    any additional class extras provided, and temporary ``_class_node`` /
    ``_import_mappings`` entries for deferred analysis.

    It also collects fully-qualified call targets for every module-level function
    so that transitive helper discovery can be done without re-reading the file.

    :param module_filepath: The file path of the module.
    :param module_name: Fully-qualified module name.
    :param class_extras: Additional information to include in each class's registry.

    :return: A tuple of (class_registry, function_calls) where *function_calls*
        maps each ``module.function_name`` to the set of fully-qualified calls it makes.
    """
    with open(module_filepath) as file:
        ast_obj = ast.parse(file.read())

    import_mappings = get_import_mappings(ast_obj)
    module_class_registry = {
        f"{module_name}.{node.name}": {
            "methods": {n.name for n in ast.walk(node) if isinstance(n, ast.FunctionDef)},
            "base_classes": [
                import_mappings.get(b.id, f"{module_name}.{b.id}")
                for b in node.bases
                if isinstance(b, ast.Name)
            ],
            "_class_node": node,
            "_import_mappings": import_mappings,
            **{
                key: callable_(class_node=node, import_mappings=import_mappings)
                for key, callable_ in class_extras.items()
            },
        }
        for node in ast_obj.body
        if isinstance(node, ast.ClassDef)
    }
    module_function_calls = {
        f"{module_name}.{node.name}": _find_calls_in_function(node, import_mappings)
        for node in ast_obj.body
        if isinstance(node, ast.FunctionDef)
    }
    return module_class_registry, module_function_calls


def _get_methods_with_hook_level_lineage(
    class_path: str,
    class_registry: dict[str, dict[str, Any]],
    target_calls: set[str],
) -> set[str]:
    """
    Return method names that have hook-level lineage calls on this class or any base class.

    Walks the inheritance tree so that child classes are considered to have HLL when a
    base class implements it (e.g. DbApiHook._run_command → PostgresHook, MySqlHook, etc.).
    HLL is computed lazily on first access using the stored AST data.
    """
    if class_path not in class_registry:
        return set()
    info = class_registry[class_path]
    if "methods_with_hook_level_lineage" not in info:
        class_node = info.pop("_class_node", None)
        import_mappings = info.pop("_import_mappings", None)
        info["methods_with_hook_level_lineage"] = (
            find_class_methods_with_specific_calls(
                class_node=class_node,
                target_calls=target_calls,
                import_mappings=import_mappings,
            )
            if class_node is not None
            else set()
        )
    methods: set[str] = set(info["methods_with_hook_level_lineage"])
    for base_name in info.get("base_classes") or []:
        if base_name in class_registry:
            methods |= _get_methods_with_hook_level_lineage(base_name, class_registry, target_calls)
    return methods


def _has_method(
    class_path: str,
    method_names: Iterable[str],
    class_registry: dict[str, dict[str, Any]],
    ignored_classes: list[str] | None = None,
) -> bool:
    """
    Determines if a class or its bases in the registry have any of the specified methods.

    :param class_path: The path of the class to check.
    :param method_names: A list of names of methods to search for.
    :param class_registry: A dictionary representing the class registry, where each key is a class name
        and the value is its metadata.
    :param ignored_classes: A list of classes to ignore when searching. If a base class has
        OL method but is ignored, the class_path will be treated as it would not have ol methods.
    :return: True if any of the specified methods are found in the class or its base classes; False otherwise.

    Example:
    >>> example_class_registry = {
    ...     "some.module.MyClass": {"methods": {"foo", "bar"}, "base_classes": ["BaseClass"]},
    ...     "another.module.BaseClass": {"methods": {"base_foo"}, "base_classes": []},
    ... }
    >>> _has_method("some.module.MyClass", ["foo"], example_class_registry)
    True
    >>> _has_method("some.module.MyClass", ["base_foo"], example_class_registry)
    True
    >>> _has_method("some.module.MyClass", ["not_a_method"], example_class_registry)
    False
    """
    ignored_classes = ignored_classes or []
    if class_path in ignored_classes:
        return False
    if class_path in class_registry:
        if any(method in class_registry[class_path]["methods"] for method in method_names):
            return True
        for base_name in class_registry[class_path]["base_classes"]:
            if base_name in ignored_classes:
                continue
            if _has_method(base_name, method_names, class_registry, ignored_classes):
                return True
    return False


def _inherits_from(
    class_path: str,
    ancestor_path: str,
    class_registry: dict[str, dict[str, Any]],
) -> bool:
    """Check whether *class_path* inherits from *ancestor_path* (walking the registry)."""
    if class_path == ancestor_path:
        return True
    if class_path not in class_registry:
        return False
    return any(
        _inherits_from(base, ancestor_path, class_registry)
        for base in class_registry[class_path]["base_classes"]
    )


def _find_calls_in_function(func_node: ast.FunctionDef, import_mappings: dict[str, str]) -> set[str]:
    """Return fully-qualified call targets found in a single function node."""
    calls: set[str] = set()
    for sub_node in ast.walk(func_node):
        if not isinstance(sub_node, ast.Call):
            continue
        func = sub_node.func
        # Direct call: some_function(...)
        if isinstance(func, ast.Name):
            fq = import_mappings.get(func.id)
            if fq:
                calls.add(fq)
        # Chained call: some_function().method(...)
        elif (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Call)
            and isinstance(func.value.func, ast.Name)
        ):
            fq = import_mappings.get(func.value.func.id)
            if fq:
                calls.add(f"{fq}.{func.attr}")
    return calls


def _compute_transitive_closure(function_calls: dict[str, set[str]], root_targets: set[str]) -> set[str]:
    """
    Expand *root_targets* with module-level functions that transitively call them.

    :param function_calls: Mapping of fully-qualified function names to the set of fully-qualified calls
     each function makes (as collected during module scanning).
    :param root_targets: The seed set of call targets (e.g. ``get_hook_lineage_collector().add_extra``).
    :return: Expanded set that includes *root_targets* plus any discovered wrapper functions.
    """
    targets = set(root_targets)
    changed = True
    while changed:
        changed = False
        for fq_name, calls in function_calls.items():
            if fq_name not in targets and calls & targets:
                targets.add(fq_name)
                changed = True
    return targets


def _get_providers_class_registry(
    class_extras: dict[str, Callable] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, set[str]]]:
    """
    Builds a registry of classes and module-level function call graph from YAML configuration files.

    This function scans through YAML configuration files to build a registry of classes.
    It parses each YAML file to get the provider's name and registers classes from Python
    module files within the provider's directory, excluding '__init__.py'.

    :return: A tuple of (class_registry, function_calls) where *function_calls* maps
        each fully-qualified module-level function to the set of calls it makes.
    """
    class_registry: dict[str, dict[str, Any]] = {}
    function_calls: dict[str, set[str]] = {}
    for provider_yaml_content in load_package_data():
        provider_pkg_root = Path(provider_yaml_content["package-dir"])
        for root, _, file_names in os.walk(provider_pkg_root):
            folder = Path(root)
            for file_name in file_names:
                if not file_name.endswith(".py") or file_name == "__init__.py":
                    continue

                module_filepath = folder.joinpath(file_name)

                module_registry, module_func_calls = _get_module_class_registry(
                    module_filepath=module_filepath,
                    module_name=(
                        provider_yaml_content["python-module"]
                        + "."
                        + module_filepath.relative_to(provider_pkg_root)
                        .with_suffix("")
                        .as_posix()
                        .replace("/", ".")
                    ),
                    class_extras={
                        "provider_name": lambda **kwargs: provider_yaml_content["package-name"],
                        "provider_version": lambda **kwargs: provider_yaml_content["versions"][0],
                        **(class_extras or {}),
                    },
                )
                class_registry.update(module_registry)
                function_calls.update(module_func_calls)

    return class_registry, function_calls


def _render_openlineage_supported_classes_content():
    openlineage_operator_methods = ("get_openlineage_facets_on_complete", "get_openlineage_facets_on_start")
    openlineage_db_hook_methods = (
        "get_openlineage_database_info",
        "get_openlineage_database_specific_lineage",
    )
    hook_lineage_collector_path = "airflow.providers.common.compat.lineage.hook.get_hook_lineage_collector"
    hook_level_lineage_root_calls = {
        f"{hook_lineage_collector_path}.add_input_asset",  # Airflow 3
        f"{hook_lineage_collector_path}.add_output_asset",  # Airflow 3
        f"{hook_lineage_collector_path}.add_input_dataset",  # Airflow 2
        f"{hook_lineage_collector_path}.add_output_dataset",  # Airflow 2
        f"{hook_lineage_collector_path}.add_extra",
    }

    class_registry, function_calls = _get_providers_class_registry()

    # Auto-discover module-level wrapper functions (e.g. send_sql_hook_lineage) that
    # transitively call the root targets, so they don't need to be listed manually.
    hook_level_lineage_collector_calls = _compute_transitive_closure(
        function_calls, hook_level_lineage_root_calls
    )

    base_sql_hook_class_path = "airflow.providers.common.sql.hooks.sql.DbApiHook"
    base_sql_op_class_path = "airflow.providers.common.sql.operators.sql.BaseSQLOperator"

    providers: dict[str, dict[str, Any]] = {}
    db_hooks: list[tuple[str, str]] = []
    db_operators: list[str] = []
    for class_path, info in class_registry.items():
        class_name = class_path.split(".")[-1]
        if class_name.startswith("_"):
            continue
        provider_entry = providers.setdefault(
            info["provider_name"],
            {"operators": {}, "db_operators": {}, "hooks": {}, "version": info["provider_version"]},
        )

        if class_name.lower().endswith("operator"):
            if _has_method(  # Operators that have OL methods NOT from BaseSQlOperator inheritance
                class_path=class_path,
                method_names=openlineage_operator_methods,
                class_registry=class_registry,
                ignored_classes=[base_sql_op_class_path],  # Exclude child classes of BaseSQlOperator
            ):
                provider_entry["operators"][class_path] = [
                    f"{class_path}.{method}"
                    for method in set(openlineage_operator_methods) & set(info["methods"])
                ]
            elif class_path == base_sql_op_class_path:
                continue  # Explicitly skip BaseSQlOperator - it's documented manually.
            elif _has_method(  # Operators that have OL methods from BaseSQlOperator inheritance
                class_path=class_path,
                method_names=openlineage_operator_methods,
                class_registry=class_registry,
                ignored_classes=[],  # Do not exclude child classes of BaseSQlOperator
            ):
                provider_entry["db_operators"][class_path] = [
                    f"{class_path}.{method}"
                    for method in set(openlineage_operator_methods) & set(info["methods"])
                ]
                db_operators.append(class_path)
        elif class_name.lower().endswith("hook"):
            if _has_method(
                class_path=class_path,
                method_names=openlineage_db_hook_methods,
                class_registry=class_registry,
                ignored_classes=[base_sql_hook_class_path],
            ) and _inherits_from(class_path, base_sql_hook_class_path, class_registry):
                db_type = (  # Extract db type from hook name
                    class_name.replace("RedshiftSQL", "Redshift")  # for RedshiftSQLHook
                    .replace("DatabricksSql", "Databricks")  # for DatabricksSqlHook
                    .replace("SnowflakeSqlApi", "SnowflakeApi")  # for SnowflakeSqlApiHook
                    .replace("Hook", "")  # for others like MySqlHook, TrinoHook etc.
                )
                db_hooks.append((db_type, class_path))

            hll_methods = _get_methods_with_hook_level_lineage(
                class_path, class_registry, hook_level_lineage_collector_calls
            )
            if hll_methods:
                provider_entry["hooks"][class_path] = [
                    f"{class_path}.{method}" for method in hll_methods if not method.startswith("_")
                ]

    providers = {
        provider: {
            "operators": {
                operator: sorted(methods)
                for operator, methods in sorted(
                    details["operators"].items(), key=lambda x: x[0].split(".")[-1]
                )
            },
            "db_operators": {
                operator: sorted(methods)
                for operator, methods in sorted(
                    details["db_operators"].items(), key=lambda x: x[0].split(".")[-1]
                )
            },
            "hooks": {
                hook: sorted(methods)
                for hook, methods in sorted(details["hooks"].items(), key=lambda x: x[0].split(".")[-1])
            },
            "version": details["version"],
        }
        for provider, details in sorted(providers.items())
        # Below filters out providers with empty 'operators', 'db_operators' and 'hooks'
        if details["hooks"] or details["operators"] or details["db_operators"]
    }
    db_hooks = sorted({hook: db_type for db_type, hook in db_hooks}.items(), key=lambda x: x[1])

    return _render_template(
        "openlineage.rst.jinja2",
        providers=providers,
        db_hooks=db_hooks,
    )


class OpenLineageSupportedClassesDirective(BaseJinjaReferenceDirective):
    """Generate list of classes supporting OpenLineage"""

    def render_content(self, *, tags: set[str] | None, header_separator: str = DEFAULT_HEADER_SEPARATOR):
        return _render_openlineage_supported_classes_content()


def setup(app):
    """Setup plugin"""
    app.add_directive("airflow-providers-openlineage-supported-classes", OpenLineageSupportedClassesDirective)

    return {"parallel_read_safe": True, "parallel_write_safe": True}
