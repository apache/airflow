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
import importlib.util
import inspect
import itertools
import pathlib
import sys

import yaml
from yaml import SafeLoader

ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]

provider_files_pattern = pathlib.Path(ROOT_DIR, "airflow", "providers").rglob("provider.yaml")
errors = []

OPERATORS = ["sensors", "operators"]
CLASS_IDENTIFIERS = ["sensor", "operator"]

TEMPLATE_TYPES = ["template_fields"]


class InstanceFieldExtractor(ast.NodeVisitor):
    def __init__(self):
        self.current_class = None
        self.instance_fields = []

    def visit_FunctionDef(self, node):
        if node.name == "__init__":
            for body in node.body:
                if isinstance(body, ast.Assign):
                    self.visit_Assign(body)
                elif isinstance(body, ast.AnnAssign):
                    self.visit_AnnAssign(body)

    def visit_Assign(self, node):
        fields = []
        for target in node.targets:
            if isinstance(target, ast.Attribute):
                fields.append(target.attr)
        if fields:
            self.instance_fields.extend(fields)

    def visit_AnnAssign(self, node):
        if isinstance(node.target, ast.Attribute):
            self.instance_fields.append(node.target.attr)


def get_template_fields_and_class_instance_fields(cls):
    all_template_fields = []
    class_instance_fields = []

    all_classes = cls.__mro__
    for current_class in all_classes:
        if current_class.__init__ is not object.__init__:
            for template_type in TEMPLATE_TYPES:
                values = current_class.__dict__
                fields = values.get(template_type)
                if fields:
                    all_template_fields.extend(fields)

            tree = ast.parse(inspect.getsource(current_class))
            visitor = InstanceFieldExtractor()
            visitor.visit(tree)
            if visitor.instance_fields:
                class_instance_fields.extend(visitor.instance_fields)
    return all_template_fields, class_instance_fields


def load_yaml_data():
    package_paths = sorted(str(path) for path in provider_files_pattern)
    result = {}
    for provider_yaml_path in package_paths:
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
        rel_path = pathlib.Path(provider_yaml_path).relative_to(ROOT_DIR).as_posix()
        result[rel_path] = provider
    return result


def get_providers_modules():
    modules_container = []
    result = load_yaml_data()

    for (_, provider_data), resource_type in itertools.product(result.items(), OPERATORS):
        if provider_data.get(resource_type):
            for data in provider_data.get(resource_type):
                modules_container.extend(data.get("python-modules"))

    return modules_container


def is_class_eligible(name):
    for op in CLASS_IDENTIFIERS:
        if name.lower().endswith(op):
            return True
    return False


def get_eligible_classes(all_classes):
    eligible_classes = [(name, cls) for name, cls in all_classes if is_class_eligible(name)]
    return eligible_classes


def iter_check_template_fields(module):
    current_module_classes = []
    imported_module = importlib.import_module(module)
    classes = inspect.getmembers(imported_module, inspect.isclass)

    eligible_classes = get_eligible_classes(classes)

    for class_name, cls in eligible_classes:
        if cls.__module__ == module:
            current_module_classes.append(class_name)

    for class_name in current_module_classes:
        print(f"Validating template fields in {module} {class_name}")
        current_cls = getattr(imported_module, class_name)
        templated_fields, class_instance_fields = get_template_fields_and_class_instance_fields(current_cls)

        if templated_fields:
            for field in templated_fields:
                if field not in class_instance_fields:
                    errors.append(f"{module}: {class_name}: {field}")


def main():
    modules = get_providers_modules()

    [iter_check_template_fields(module) for module in modules]
    if errors:
        print("Invalid template fields found:")
        for error in errors:
            print(error)

    return len(errors)


if __name__ == "__main__":
    sys.exit(main())
