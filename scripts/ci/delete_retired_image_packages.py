#!/usr/bin/env python3
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
"""
Delete the CI and PROD image packages of branches and Python versions that are no longer built.

Every branch and Python version keeps its ``<repo>/<branch>/(ci|prod)/python<version>`` package in
ghcr.io after it is retired, with all its versions. A package is deleted unless its branch is in
``--keep-branches`` and its Python version in ``--keep-python``; ``--only`` narrows the run to the
packages matching one of its glob patterns, for example to delete a single package first. Deleting a
package removes all of its versions in one call; GitHub can restore it within 30 days unless the
name is pushed to again.

Usage: delete_retired_image_packages.py --keep-branches "main v3-4-test"
           --keep-python "3.11 3.12 3.13 3.14" [--only "airflow/v2-*"] [--dry-run]
Environment: GITHUB_REPOSITORY (``owner/repo``) and a token for ``gh`` that may delete the packages.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote


def get_image_package_pattern(repo: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(repo)}/(?P<branch>[^/]+)/(ci|prod)/python(?P<python>[0-9.]+)$")


def find_retired_packages(
    names: list[str],
    repo: str,
    keep_branches: set[str],
    keep_python: set[str],
    only: list[str] | None = None,
) -> list[str]:
    pattern = get_image_package_pattern(repo)
    retired = []
    for name in names:
        match = pattern.match(name)
        if not match:
            continue
        if match["branch"] in keep_branches and match["python"] in keep_python:
            continue
        if only and not any(fnmatch.fnmatchcase(name, glob) for glob in only):
            continue
        retired.append(name)
    return sorted(retired)


def _gh_api(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["gh", "api", *args], check=False, capture_output=True, text=True)


def _delete_package(owner: str, name: str) -> tuple[str, str | None]:
    result = _gh_api(["-X", "DELETE", f"/orgs/{owner}/packages/container/{quote(name, safe='')}"])
    return name, None if result.returncode == 0 else (result.stderr.strip() or result.stdout.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--keep-branches", required=True, help="Space-separated branches to keep")
    parser.add_argument("--keep-python", required=True, help="Space-separated Python versions to keep")
    parser.add_argument(
        "--only", default="", help="Space-separated glob patterns; delete only the packages matching one"
    )
    parser.add_argument("--parallel", type=int, default=4, help="Deletions to run at once")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")
    args = parser.parse_args()

    keep_branches = set(args.keep_branches.split())
    keep_python = set(args.keep_python.split())
    if not keep_branches or not keep_python:
        print("Refusing to run without branches and Python versions to keep.")
        return 1

    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/", 1)
    listing = _gh_api(
        ["--paginate", f"/orgs/{owner}/packages?package_type=container&per_page=100", "--jq", ".[].name"]
    )
    if listing.returncode != 0:
        print(f"Cannot list the packages: {listing.stderr.strip()}")
        return 1
    retired = find_retired_packages(
        listing.stdout.split(), repo, keep_branches, keep_python, args.only.split()
    )
    print(f"{len(retired)} retired image package(s), keeping {sorted(keep_branches)} x {sorted(keep_python)}")
    if args.dry_run:
        for name in retired:
            print(f"Would delete {name}")
        return 0

    failed = 0
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        for name, error in executor.map(lambda name: _delete_package(owner, name), retired):
            if error:
                failed += 1
                print(f"Could not delete {name}: {error}")
            else:
                print(f"Deleted {name}")
    print(f"{len(retired) - failed} deleted, {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
