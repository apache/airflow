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

import json
import os.path
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import console, get_provider_base_dir_from_path, get_provider_id_from_path


def check_imports() -> bool:
    errors_found = False
    cmd = [
        "ruff",
        "analyze",
        "graph",
        "--python",
        sys.executable,
    ]
    console.print("Cmd", cmd)
    import_tree_str = subprocess.check_output(cmd)
    import_tree = json.loads(import_tree_str)
    # Uncomment these if you want to debug strange dependencies and see if ruff gets it right
    console.print("Dependencies discovered by ruff:")
    console.print(import_tree)

    for importing_file in sys.argv[1:]:
        if not importing_file.startswith("providers/"):
            console.print(f"[yellow]Skipping non-provider file: {importing_file}")
            continue
        importing_file_path = Path(importing_file)
        console.print(importing_file_path)
        imported_files_array = import_tree.get(importing_file, None)
        if imported_files_array is None:
            continue
        imported_file_paths = [Path(file) for file in imported_files_array]
        for imported_file_path in imported_file_paths:
            if imported_file_path.name == "version_compat.py":
                # Note - this will check also imports from other places - not only from providers
                # Which means that import from tests_common, and airflow will be also banned
                common_path = os.path.commonpath([importing_file, imported_file_path.as_posix()])
                imported_file_parent_dir = imported_file_path.parent.as_posix()
                if common_path != imported_file_parent_dir:
                    provider_id = get_provider_id_from_path(importing_file_path)
                    provider_dir = get_provider_base_dir_from_path(importing_file_path)
                    console.print(
                        f"\n[red]Invalid import of `version_compat` module in provider {provider_id} in:\n"
                    )
                    console.print(f"[yellow]{importing_file_path}")
                    console.print(
                        f"\n[bright_blue]The AIRFLOW_V_X_Y_PLUS import should be "
                        f"from the {provider_id} provider root directory ({provider_dir}), but it is currently from:"
                    )
                    console.print(f"\n[yellow]{imported_file_path}\n")
                    console.print(
                        f"1. Copy `version_compat`.py to `{provider_dir}/version_compat.py` if not there.\n"
                        f"2. Import the version constants you need as:\n\n"
                        f"[yellow]from airflow.providers.{provider_id}.version_compat import ...[/]\n"
                        f"\n"
                    )
                    errors_found = True
    return errors_found


if check_imports():
    console.print("\n[red]Errors found in imports![/]\n")
    sys.exit(1)
else:
    console.print("\n[green]All version_compat imports are correct![/]\n")
