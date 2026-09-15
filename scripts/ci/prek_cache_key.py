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

"""Compute a cache identity for prek's host-side hook environments."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
import sysconfig
from collections.abc import Mapping
from pathlib import Path

REQUIRED_INPUTS = ("PLATFORM", "UV_VERSION", "PREK_VERSION", "PREK_CONFIG_HASH", "GITHUB_WORKSPACE")


def build_cache_identity(environ: Mapping[str, str]) -> dict[str, str]:
    """Do not reuse virtualenvs across incompatible hosts or absolute interpreter paths."""
    missing = [name for name in REQUIRED_INPUTS if not environ.get(name)]
    if missing:
        raise ValueError(f"Missing prek cache inputs: {', '.join(missing)}")
    try:
        os_release = platform.freedesktop_os_release()
    except OSError:
        os_release = {}
    return {
        **{name: environ[name] for name in REQUIRED_INPUTS},
        "system": platform.system(),
        "machine": platform.machine(),
        "os_id": os_release.get("ID", ""),
        "os_version": os_release.get("VERSION_ID", ""),
        "python_version": platform.python_version(),
        "python_abi": sysconfig.get_config_var("SOABI") or "",
        "python_executable": str(Path(sys.executable).resolve()),
        "python_prefix": sys.prefix,
        "home": str(Path.home()),
    }


def compute_cache_key(identity: Mapping[str, str]) -> str:
    """Use a bounded-length, deterministic artifact name without whitespace or path separators."""
    payload = json.dumps(dict(identity), sort_keys=True, separators=(",", ":")).encode()
    return "cache-prek-v11-" + hashlib.sha256(payload).hexdigest()


def main() -> None:
    key = compute_cache_key(build_cache_identity(os.environ))
    print(f"Prek cache key: {key}")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"key={key}\n")


if __name__ == "__main__":
    main()
