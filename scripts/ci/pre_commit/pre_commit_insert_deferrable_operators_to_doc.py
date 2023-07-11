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
from __future__ import annotations

import ast
import glob
import itertools
import os
import sys
from pathlib import Path
from typing import Iterator

import yaml
from jinja2 import BaseLoader, Environment

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
# flake8: noqa: F401
from common_precommit_utils import insert_documentation  # isort: skip


template = """
**{{ pacakge_name }}**

.. list-table::
   :header-rows: 1

   * - Modules
     - Operators
   {% for module, operator in operators %}
   * - :mod:`{{ module }}`
     - :mod:`{{ operator}}`
   {% endfor %}

"""

env = Environment(loader=BaseLoader()).from_string(template)
doc_path = AIRFLOW_SOURCES_ROOT / "docs/apache-airflow/deferrable-operator-ref.rst"


def iter_deferrable_operators(module_filename: str) -> Iterator[tuple[str, str]]:
    ast_obj = ast.parse(open(module_filename).read())
    cls_nodes = (node for node in ast.iter_child_nodes(ast_obj) if isinstance(node, ast.ClassDef))
    init_method_nodes = (
        (cls_node, node)
        for cls_node in cls_nodes
        for node in ast.iter_child_nodes(cls_node)
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )

    for cls_node, node in init_method_nodes:
        args = node.args
        for argument in [*args.args, *args.kwonlyargs]:
            if argument.arg == "deferrable":
                module_name = module_filename[:-3].replace("/", ".")
                op_name = cls_node.name
                yield (module_name, op_name)


def main():
    airflow_op_module_paths = itertools.chain(
        glob.glob("airflow/operators/**.py", recursive=True),
        glob.glob("airflow/sensors/**.py", recursive=True),
    )
    airflow_deferrable_ops = [
        op for module in airflow_op_module_paths for op in iter_deferrable_operators(module)
    ]

    content = [env.render(pacakge_name="apache-airflow", operators=airflow_deferrable_ops)]

    provider_yamls = glob.glob("airflow/providers/**/provider.yaml", recursive=True)
    for provider_yaml in provider_yamls:
        provider_root = Path(provider_yaml).parent
        operators = []
        for root, _, file_names in os.walk(provider_root):
            if all([target not in root for target in ["operators", "sensors"]]):
                continue

            for file_name in file_names:
                if not file_name.endswith(".py") or file_name == "__init__.py":
                    continue
                operators.extend(iter_deferrable_operators(f"{root}/{file_name}"))

        if operators:
            provider_yaml_content = yaml.safe_load(Path(provider_yaml).read_text())
            package_name = provider_yaml_content["package-name"]
            content.append(env.render(pacakge_name=package_name, operators=operators))

    insert_documentation(
        doc_path,
        content,
        header=".. START automatically generated",
        footer=".. END automatically generated",
    )


if __name__ == "__main__":
    main()
