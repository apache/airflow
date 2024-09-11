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
from pathlib import Path
from typing import Any, Iterable

import yaml

# No stub exists for docutils.parsers.rst.directives. See https://github.com/python/typeshed/issues/5755.
from provider_yaml_utils import get_provider_yaml_paths

from docs.exts.operators_and_hooks_ref import (
    DEFAULT_HEADER_SEPARATOR,
    BaseJinjaReferenceDirective,
    _render_template,
)


def get_import_mappings(tree):
    """Retrieve a mapping of local import names to their fully qualified module paths from an AST tree.

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
    module_filepath: str, class_extras: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """Extracts classes and its information from a Python module file.

    The function parses the specified module file and registers all classes.
    The registry for each class includes the module filename, methods, base classes
    and any additional class extras provided.

    :param module_filepath: The file path of the module.
    :param class_extras: Additional information to include in each class's registry.

    :return: A dictionary with class names as keys and their corresponding information.
    """
    with open(module_filepath) as file:
        ast_obj = ast.parse(file.read())

    module_name = module_filepath.replace("/", ".").replace(".py", "").lstrip(".")
    import_mappings = get_import_mappings(ast_obj)
    module_class_registry = {
        f"{module_name}.{node.name}": {
            "methods": {n.name for n in ast.walk(node) if isinstance(n, ast.FunctionDef)},
            "base_classes": [
                import_mappings.get(b.id, f"{module_name}.{b.id}")
                for b in node.bases
                if isinstance(b, ast.Name)
            ],
            **class_extras,
        }
        for node in ast_obj.body
        if isinstance(node, ast.ClassDef)
    }
    return module_class_registry


def _has_method(
    class_path: str, method_names: Iterable[str], class_registry: dict[str, dict[str, Any]]
) -> bool:
    """Determines if a class or its bases in the registry have any of the specified methods.

    :param class_path: The path of the class to check.
    :param method_names: A list of names of methods to search for.
    :param class_registry: A dictionary representing the class registry, where each key is a class name
                            and the value is its metadata.
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
    if class_path in class_registry:
        if any(method in class_registry[class_path]["methods"] for method in method_names):
            return True
        for base_name in class_registry[class_path]["base_classes"]:
            if _has_method(base_name, method_names, class_registry):
                return True
    return False


def _get_providers_class_registry() -> dict[str, dict[str, Any]]:
    """Builds a registry of classes from YAML configuration files.

    This function scans through YAML configuration files to build a registry of classes.
    It parses each YAML file to get the provider's name and registers classes from Python
    module files within the provider's directory, excluding '__init__.py'.

    :return: A dictionary with provider names as keys and a dictionary of classes as values.
    """
    class_registry = {}
    for provider_yaml_path in get_provider_yaml_paths():
        provider_yaml_content = yaml.safe_load(Path(provider_yaml_path).read_text())
        for root, _, file_names in os.walk(Path(provider_yaml_path).parent):
            for file_name in file_names:
                module_filepath = f"{os.path.relpath(root)}/{file_name}"
                if not module_filepath.endswith(".py") or module_filepath == "__init__.py":
                    continue

                module_registry = _get_module_class_registry(
                    module_filepath=module_filepath,
                    class_extras={"provider_name": provider_yaml_content["package-name"]},
                )
                class_registry.update(module_registry)

    return class_registry


def _render_openlineage_supported_classes_content():
    openlineage_operator_methods = ("get_openlineage_facets_on_complete", "get_openlineage_facets_on_start")
    openlineage_db_hook_methods = (
        "get_openlineage_database_info",
        "get_openlineage_database_specific_lineage",
    )

    class_registry = _get_providers_class_registry()
    # These excluded classes will be included in docs directly
    class_registry.pop("airflow.providers.common.sql.hooks.sql.DbApiHook")
    class_registry.pop("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator")

    providers: dict[str, dict[str, list[str]]] = {}
    db_hooks: list[tuple[str, str]] = []
    for class_path, info in class_registry.items():
        class_name = class_path.split(".")[-1]
        if class_name.startswith("_"):
            continue
        provider_entry = providers.setdefault(info["provider_name"], {"operators": []})

        if class_name.lower().endswith("operator"):
            if _has_method(
                class_path=class_path,
                method_names=openlineage_operator_methods,
                class_registry=class_registry,
            ):
                provider_entry["operators"].append(class_path)
        elif class_name.lower().endswith("hook"):
            if _has_method(
                class_path=class_path,
                method_names=openlineage_db_hook_methods,
                class_registry=class_registry,
            ):
                db_type = class_name.replace("SqlApiHook", "").replace("Hook", "")
                db_hooks.append((db_type, class_path))

    providers = {
        provider: {key: sorted(set(value), key=lambda x: x.split(".")[-1]) for key, value in details.items()}
        for provider, details in sorted(providers.items())
        if any(details.values())  # This filters out providers with empty 'operators'
    }
    db_hooks = sorted({db_type: hook for db_type, hook in db_hooks}.items(), key=lambda x: x[0])

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
