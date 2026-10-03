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

import hashlib
import json
import subprocess
from pathlib import Path

from airflow_breeze.global_constants import FILES_FOR_REBUILD_CHECK

ENVIRONMENT_KEY_SCHEMA_VERSION = 1


def calculate_ci_environment_fingerprint(root: Path, python: str, platform: str) -> dict:
    tracked_paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=root, text=True).split("\0")
    paths = set(FILES_FOR_REBUILD_CHECK) | {"uv.lock"}
    paths.update(path for path in tracked_paths if Path(path).name in {"pyproject.toml", "provider.yaml"})
    inputs = {
        "schema_version": ENVIRONMENT_KEY_SCHEMA_VERSION,
        "python": python,
        "platform": platform,
        "files": {
            path: hashlib.sha256((root / path).read_bytes()).hexdigest()
            if (root / path).is_file()
            else "missing"
            for path in sorted(paths)
        },
    }
    canonical_inputs = json.dumps(inputs, sort_keys=True, separators=(",", ":")).encode()
    return {
        "key": hashlib.sha256(canonical_inputs).hexdigest(),
        "inputs": inputs,
        "reuse_eligible": False,
        "unresolved_inputs": [
            "resolved_base_image_digest",
            "effective_semantic_build_parameters",
            "complete_environment_file_inputs",
            "external_dependency_inputs",
        ],
    }
