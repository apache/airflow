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

import ast
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, NamedTuple

from jinja2 import BaseLoader, Environment
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
CONTRIB_DIR = AIRFLOW_SOURCES_ROOT / "airflow" / "contrib"


class Import(NamedTuple):
    module: str
    name: str
    alias: str


def get_imports(path: Path):
    root = ast.parse(path.read_text())
    for node in ast.iter_child_nodes(root):
        if isinstance(node, ast.Import):
            module: List[str] = []
        elif isinstance(node, ast.ImportFrom) and node.module:
            module = node.module.split('.')
        else:
            continue
        for n in node.names:  # type: ignore[attr-defined]
            yield Import(".".join(module), n.name, n.asname if n.asname else n.name)


DEPRECATED_CLASSES_TEMPLATE = """
__deprecated_classes = {
{%- for module, package_imports in package_imports.items() %}
    '{{module}}': {
{%- for import_item in package_imports %}
        '{{import_item.alias}}': '{{import_item.module}}.{{import_item.name}}',
{%- endfor %}
    },
{%- endfor %}
}
"""


if __name__ == '__main__':
    console = Console(color_system="standard", width=300)
    all_deprecated_imports: Dict[str, Dict[str, List[Import]]] = defaultdict(lambda: defaultdict(list))
    for contrib_file_path in CONTRIB_DIR.rglob("*.py"):
        if contrib_file_path.name == '__init__.py':
            continue
        original_module = os.fspath(contrib_file_path.parent.relative_to(CONTRIB_DIR)).replace(os.sep, ".")
        for _import in get_imports(contrib_file_path):
            module_name = contrib_file_path.name[: -len(".py")]
            if _import.name not in ['warnings']:
                all_deprecated_imports[original_module][module_name].append(_import)

    for module, package_imports in all_deprecated_imports.items():
        console.print(f"[yellow]Import dictionary for {module}:\n")
        template = Environment(loader=BaseLoader()).from_string(DEPRECATED_CLASSES_TEMPLATE)
        console.print(template.render(package_imports=dict(sorted(package_imports.items()))))
