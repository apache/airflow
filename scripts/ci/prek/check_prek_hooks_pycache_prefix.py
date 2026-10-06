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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pyyaml>=6.0.3",
# ]
# ///
"""Fail when a prek hook does not redirect Python bytecode out of the source tree.

prek runs hooks in parallel, and breeze removes every ``__pycache__`` under the
repository on start-up. A hook that writes ``.pyc`` files next to the sources can
refill a directory while a concurrent breeze is removing it, failing that hook
with ``Directory not empty``. Every hook therefore sets ``PYTHONPYCACHEPREFIX``
to a hidden ``.build/`` directory, which breeze skips and git ignores.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

PYCACHE_PREFIX = ".build/pycache"


def find_hooks_without_pycache_prefix(config_path: Path) -> list[str]:
    config = yaml.safe_load(config_path.read_text()) or {}
    return [
        hook["id"]
        for repo in config.get("repos", [])
        for hook in repo.get("hooks", [])
        if (hook.get("env") or {}).get("PYTHONPYCACHEPREFIX") != PYCACHE_PREFIX
    ]


def main() -> int:
    config_paths = subprocess.check_output(
        ["git", "ls-files", "*.pre-commit-config.yaml", ".pre-commit-config.yaml"], text=True
    ).split()
    errors = [
        f"{config_path}: {hook_id}"
        for config_path in config_paths
        for hook_id in find_hooks_without_pycache_prefix(Path(config_path))
    ]
    if errors:
        print("ERROR: These prek hooks do not set PYTHONPYCACHEPREFIX:\n")
        print("\n".join(f"  {error}" for error in errors))
        print(
            "\nAdd the following to each of them, right after `name:`:\n\n"
            "        env:\n"
            f"          PYTHONPYCACHEPREFIX: {PYCACHE_PREFIX}\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
