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
from __future__ import annotations

import ast
import functools
import importlib.util
import inspect
import itertools
import pathlib
import sys
import warnings

import yaml
from rich.console import Console

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore

console = Console(width=400, color_system="standard")
ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]

provider_files_pattern = pathlib.Path(ROOT_DIR, "providers").rglob("provider.yaml")

OPERATORS: list[str] = ["sensors", "operators"]
CLASS_IDENTIFIERS: list[str] = ["sensor", "operator"]

TEMPLATE_TYPES: list[str] = ["template_fields"]


class InstanceFieldExtractor(ast.NodeVisitor):
    def __init__(self):
        self.current_class = None
        self.instance_fields = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        if node.name == "__init__":
            self.generic_visit(node)
        return node

    def visit_Assign(self, node: ast.Assign) -> ast.Assign:
        fields = []
        for target in node.targets:
            if isinstance(target, ast.Attribute):
                fields.append(target.attr)
        if fields:
            self.instance_fields.extend(fields)
        return node

    def visit_AnnAssign(self, node: ast.AnnAssign) -> ast.AnnAssign:
        if isinstance(node.target, ast.Attribute):
            self.instance_fields.append(node.target.attr)
        return node


@functools.cache
def get_instance_fields_by_qualname(source_file: str) -> dict[str, tuple[str, ...]]:
    """
    Parse one source file and map every class's qualified name to the ``self.<field>`` names
    assigned in its ``__init__``.

    Cached per file because base classes such as ``BaseOperator`` sit in the MRO of every operator,
    and ``inspect.getsource`` on a class re-parses its whole module to locate it. Parsing each file
    once and indexing all of its classes replaces both of those repeated parses.
    """
    result: dict[str, tuple[str, ...]] = {}

    def index_classes(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                qualname = f"{prefix}{child.name}"
                visitor = InstanceFieldExtractor()
                visitor.visit(child)
                # Like inspect.getsource, the first definition wins when a name is defined twice.
                result.setdefault(qualname, tuple(visitor.instance_fields))
                index_classes(child, f"{qualname}.")

    index_classes(ast.parse(pathlib.Path(source_file).read_text()), "")
    return result


def get_class_instance_fields(current_class: type) -> tuple[str, ...]:
    """Return the ``self.<field>`` names assigned in the class's own ``__init__``."""
    source_file = inspect.getsourcefile(current_class)
    if source_file is None:
        raise OSError(f"could not find source file for {current_class!r}")
    return get_instance_fields_by_qualname(source_file).get(current_class.__qualname__, ())


def get_template_fields_and_class_instance_fields(cls):
    """
    1.This method retrieves the operator class and obtains all its parent classes using the method resolution order (MRO).
    2. It then gathers the templated fields declared in both the operator class and its parent classes.
    3. Finally, it retrieves the instance fields of the operator class, specifically the self.fields attributes.
    """
    all_template_fields = []
    class_instance_fields = []

    all_classes = cls.__mro__
    for current_class in all_classes:
        if current_class.__init__ is not object.__init__:
            cls_attr = current_class.__dict__
            for template_type in TEMPLATE_TYPES:
                fields = cls_attr.get(template_type)
                if fields:
                    all_template_fields.extend(fields)

            class_instance_fields.extend(get_class_instance_fields(current_class))
    return all_template_fields, class_instance_fields


def load_yaml_data() -> dict:
    """
    It loads all the provider YAML files and retrieves the module referenced within each YAML file.
    """
    package_paths = sorted(str(path) for path in provider_files_pattern)
    result = {}
    for provider_yaml_path in package_paths:
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
        rel_path = pathlib.Path(provider_yaml_path).relative_to(ROOT_DIR).as_posix()
        result[rel_path] = provider
    return result


def get_providers_modules() -> list[str]:
    modules_container = []
    result = load_yaml_data()

    for (_, provider_data), resource_type in itertools.product(result.items(), OPERATORS):
        if provider_data.get(resource_type):
            for data in provider_data.get(resource_type):
                modules_container.extend(data.get("python-modules"))

    return modules_container


def path_to_module(pyfile: str) -> str:
    """
    Turn a repo-relative source path into the dotted module name provider.yaml uses.

    Both distributions keep their sources under ``src``, and only the part after it
    is importable::

        providers/ftp/src/airflow/providers/ftp/operators/ftp.py
            -> airflow.providers.ftp.operators.ftp
        airflow-core/src/airflow/sensors/base.py
            -> airflow.sensors.base
    """
    parts = pathlib.PurePosixPath(pyfile).with_suffix("").parts
    if "src" in parts:
        parts = parts[parts.index("src") + 1 :]
    return ".".join(parts)


def is_class_eligible(name: str) -> bool:
    for op in CLASS_IDENTIFIERS:
        if name.lower().endswith(op):
            return True
    return False


def get_eligible_classes(all_classes):
    """
    Filter the results to include only classes that end with `Sensor` or `Operator`.

    """

    eligible_classes = [(name, cls) for name, cls in all_classes if is_class_eligible(name)]
    return eligible_classes


def check_template_fields(module: str) -> list[str]:
    """
    1. This method imports the providers module and retrieves all the classes defined within it.
       Modules that only import under an optional provider dependency are skipped.
    2. It then filters and selects classes related to operators or sensors by checking if the class name ends with "Operator" or "Sensor."
    3. For each operator class, it validates the template fields by inspecting the class instance fields.

    Returns the list of errors found in the module.
    """
    with warnings.catch_warnings(record=True):
        try:
            imported_module = importlib.import_module(module)
        except AirflowOptionalProviderFeatureException:
            # The module gates a cross-provider feature behind an optional
            # dependency that is not installed here (e.g. the Google Dataflow
            # operators need apache-airflow-providers-apache-beam). There are no
            # classes to inspect, and a missing optional provider is not what
            # this hook reports on.
            return []
        classes = inspect.getmembers(imported_module, inspect.isclass)
    op_classes = get_eligible_classes(classes)

    module_errors: list[str] = []
    for op_class_name, cls in op_classes:
        if cls.__module__ == module:
            templated_fields, class_instance_fields = get_template_fields_and_class_instance_fields(cls)

            for field in templated_fields:
                if field not in class_instance_fields:
                    module_errors.append(f"{module}: {op_class_name}: {field}")
    return module_errors


if __name__ == "__main__":
    provider_modules = get_providers_modules()

    if len(sys.argv) > 1:
        py_files = sorted(sys.argv[1:])
        modules_to_validate = [
            module_name for pyfile in py_files if (module_name := path_to_module(pyfile)) in provider_modules
        ]
    else:
        modules_to_validate = provider_modules

    errors: list[str] = []
    for module in modules_to_validate:
        errors.extend(check_template_fields(module))
    if errors:
        console.print("[red]Found Invalid template fields:")
        for error in errors:
            console.print(f"[red]Error:[/] {error}")

    sys.exit(len(errors))
