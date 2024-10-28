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


errors: list[str] = []

EXCEPTIONS = ["airflow/providers/common/compat/openlineage/facet.py"]


def main() -> int:
    for path in sys.argv[1:]:
        import_count = 0
        local_error_count = 0
        for imp in get_imports(path):
            import_count += 1
            if len(imp.module) > 2:
                if imp.module[:3] == ["openlineage", "client", "facet"] or imp.module[
                    :3
                ] == [
                    "openlineage",
                    "client",
                    "run",
                ]:
                    if path not in EXCEPTIONS:
                        local_error_count += 1
                        errors.append(f"{path}: ({'.'.join(imp.module)})")
        console.print(
            f"[blue]{path}:[/] Import count: {import_count}, error_count {local_error_count}"
        )
    if errors:
        console.print(
            "[red]Some files imports from `openlineage.client.facet` or `openlineage.client.run`. which are deprecated.[/]\n"
            "You should import from `airflow.providers.common.compat.openlineage.facet` instead."
        )
        console.print("Error summary:")
        for error in errors:
            console.print(error)
        return 1
    else:
        console.print("[green]All good!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
