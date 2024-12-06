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

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import console, initialize_breeze_precommit

initialize_breeze_precommit(__name__, __file__)

providers: set[str] = set()

file_list = sys.argv[1:]
console.print(f"[bright_blue]Determining providers to regenerate from: {file_list}\n")

# get all folders from arguments
for examined_file in file_list:
    if not examined_file.startswith("providers/src"):
        continue
    console.print(f"[bright_blue]Looking at {examined_file} for provider.yaml")
    # find the folder where provider.yaml is
    for parent in Path(examined_file).parents:
        console.print(f"[bright_blue]Checking {parent}")
        if (parent / "provider.yaml").exists():
            provider_folder = parent
            break
    else:
        console.print(f"[yellow]\nCould not find `provider.yaml` in any parent of {examined_file}[/]")
        continue
    # find base for the provider sources
    for parent in provider_folder.parents:
        if parent.name == "providers":
            base_folder = parent
            console.print(f"[bright_blue]Found base folder {base_folder}")
            break
    else:
        console.print(f"[red]\nCould not find base folder for {provider_folder}")
        sys.exit(1)
    provider_name = ".".join(provider_folder.relative_to(base_folder).as_posix().split("/"))
    providers.add(provider_name)

console.print(f"[bright_blue]Regenerating providers __init__ files for providers: {providers}[/]")

if not providers:
    console.print("[red]\nThe found providers list cannot be empty[/]")
    sys.exit(1)

res = subprocess.run(
    [
        "breeze",
        "release-management",
        "prepare-provider-documentation",
        "--reapply-templates-only",
        "--skip-git-fetch",
        "--only-min-version-update",
        *list(providers),
    ],
    check=False,
)
if res.returncode != 0:
    console.print("[red]\nError while regenerating provider init files.")
    sys.exit(res.returncode)
