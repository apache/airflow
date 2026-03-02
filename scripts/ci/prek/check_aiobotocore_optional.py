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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "click>=8.1.8",
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import sys
from pathlib import Path

import yaml
from rich.console import Console

console = Console(color_system="standard", width=200)

if __name__ == "__main__":
    errors = []
    for file in sys.argv[1:]:
        console.print(f"[blue]Checking[/blue]: {file}")
        provider_yaml_content = yaml.safe_load(Path(file).read_text())
        dependencies = provider_yaml_content.get("dependencies")
        if dependencies and any(dependency.startswith("aiobotocore") for dependency in dependencies):
            errors.append(
                f"\n[red]Error: the aibotocore cannot be a required dependency, "
                f"because it restricts botocore too much[/]\n\n"
                f"The [magenta]{file}[/] file has aiobotocore dependency set at top level.\n\n"
                f"[yellow]Please remove it and make sure it is added only as "
                f"an optional dependency in additional-extras[/n]\n"
            )
    if errors:
        for error in errors:
            console.print(error)
        sys.exit(1)
