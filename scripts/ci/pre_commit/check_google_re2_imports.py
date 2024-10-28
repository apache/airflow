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
from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import NamedTuple

from rich.console import Console

console = Console(color_system="standard", width=200)


class ImportTuple(NamedTuple):
    module: list[str]
    name: list[str]
    alias: str


def get_imports(path: str):
    with open(path) as fh:
        root = ast.parse(fh.read(), path)

    for node in ast.iter_child_nodes(root):
        if isinstance(node, ast.Import):
            module: list[str] = node.names[0].name.split(".") if node.names else []
        elif isinstance(node, ast.ImportFrom) and node.module:
            module = node.module.split(".")
        else:
            continue

        for n in node.names:  # type: ignore[attr-defined]
            yield ImportTuple(module=module, name=n.name.split("."), alias=n.asname)


def find_provider_yaml(path: str) -> Path | None:
    cur_path = Path(path)
    while cur_path.parent != cur_path:
        if (cur_path / "provider.yaml").exists():
            return cur_path / "provider.yaml"
        cur_path = cur_path.parent
    return None


errors: list[str] = []


def main() -> int:
    for path in sys.argv[1:]:
        if not path.startswith("airflow/providers/"):
            continue
        for imp in get_imports(path):
            if imp.module[0] == "re2":
                provider_yaml = find_provider_yaml(path)
                if not provider_yaml:
                    console.print(f"[yellow]{path}:[/] Not found provider.yaml")
                    continue
                if "google-re2" not in provider_yaml.read_text():
                    console.print(
                        f"[yellow]{path}:[/] Not found google-re2 in {provider_yaml}"
                    )
                    errors.append(f"{path}:{provider_yaml}")
    if errors:
        console.print(
            "[red]Some Providers use 're2' module but do not declare 'google-re2' as dependency.[/]\n"
            "Please add google-re2 as dependency for those files in provider.yaml files."
        )
        console.print("Impacted paths:")
        for error in errors:
            console.print(error)
        return 1
    else:
        console.print("[green]All good!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
