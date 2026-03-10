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
"""Pre-commit hook that verifies K8s JSON schemas are published on airflow.apache.org.

Triggered when ``global_constants.py`` changes.  For each version in
``ALLOWED_KUBERNETES_VERSIONS``, sends a HEAD request to
``https://airflow.apache.org/k8s-schemas/v{version}-standalone-strict/configmap-v1.json``.
If any version returns non-200 the hook fails with instructions.
"""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import console, read_allowed_kubernetes_versions

PROBE_URL_TEMPLATE = "https://airflow.apache.org/k8s-schemas/v{version}-standalone-strict/configmap-v1.json"


def _print(msg: str) -> None:
    if console:
        console.print(msg)
    else:
        print(msg, file=sys.stderr)


def main() -> int:
    versions = read_allowed_kubernetes_versions()
    missing: list[str] = []

    for version in versions:
        url = PROBE_URL_TEMPLATE.format(version=version)
        req = Request(url, method="HEAD")
        try:
            resp = urlopen(req, timeout=15)
            if resp.status == 200:
                _print(f"  v{version}: published")
            else:
                _print(f"  v{version}: HTTP {resp.status}")
                missing.append(version)
        except HTTPError as e:
            _print(f"  v{version}: HTTP {e.code}")
            missing.append(version)
        except URLError as e:
            _print(f"  v{version}: {e.reason}")
            missing.append(version)

    if missing:
        _print(
            "\nK8s schemas are NOT published for the following versions:\n"
            + "\n".join(f"  - v{v}" for v in missing)
            + "\n\nTo fix this:\n"
            "  1. Check out the airflow-site repository.\n"
            "  2. Run: breeze ci upgrade --airflow-site <path-to-airflow-site>\n"
            "  3. Commit and push changes in both repos.\n"
        )
        return 1

    _print("All K8s schema versions are published.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
