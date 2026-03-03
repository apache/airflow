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

import os
import subprocess
import sys

DATAMODELS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/datamodels/"
VERSIONS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/versions/"


def get_changed_files_ci() -> list[str]:
    """Get changed files in a CI environment by comparing against the target branch."""
    target_branch = os.environ.get("GITHUB_BASE_REF", "main")
    try:
        subprocess.run(
            ["git", "fetch", "origin", target_branch],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError:
        pass
    result = subprocess.run(
        ["git", "diff", "--name-only", f"origin/{target_branch}...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.strip().splitlines() if f]


def get_changed_files_local() -> list[str]:
    """Get staged files in a local (non-CI) environment."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.strip().splitlines() if f]


def main() -> int:
    is_ci = os.environ.get("CI")
    if is_ci:
        changed_files = get_changed_files_ci()
    else:
        changed_files = get_changed_files_local()

    datamodel_files = [f for f in changed_files if f.startswith(DATAMODELS_PREFIX)]
    version_files = [f for f in changed_files if f.startswith(VERSIONS_PREFIX)]

    if datamodel_files and not version_files:
        print("ERROR: Changes to execution API datamodels require corresponding changes in versions.", file=sys.stderr)
        print("", file=sys.stderr)
        print("The following datamodel files were changed:", file=sys.stderr)
        for f in datamodel_files:
            print(f"  - {f}", file=sys.stderr)
        print("", file=sys.stderr)
        print(
            "But no files were changed under:\n"
            f"  {VERSIONS_PREFIX}\n"
            "\n"
            "Please add or update a version file to reflect the datamodel changes.\n"
            "See contributing-docs/19_execution_api_versioning.rst for details.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
