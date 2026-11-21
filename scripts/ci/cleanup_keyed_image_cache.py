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
Delete keyed CI image cache entries a while after they were first pushed.

``breeze ci-image build --prepare-buildx-cache`` pushes, next to the shared ``cache-<platform>`` tag,
a ``cache-<platform>-<dependency key>`` tag for every set of dependency inputs it builds. Those pile
up with every dependency change, so the versions of the CI image packages whose only tags are keyed
cache tags, and which were created before the cutoff, are deleted. Shared cache tags, ``latest``
and anything else are never touched.

The age is counted from creation, not from the last update: a push of identical cache content
reuses the version and only moves its ``updated_at``, and versions are listed by creation, so a
version updated after creation could otherwise fall out of the scanned window and stay forever.
A key still in use is pushed again within hours of being deleted. A version that kept another
tag past the scanned window is left to cleanup_old_image_versions.py, which walks every version.

Usage: cleanup_keyed_image_cache.py [--days N] [--dry-run]
Environment: GITHUB_REPOSITORY (``owner/repo``), and a token for ``gh`` with ``packages: write``.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

KEYED_CACHE_TAG = re.compile(r"^cache-linux-(amd64|arm64)-[0-9a-f]{16}$")


def get_ci_image_package_pattern(repo: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(repo)}/[^/]+/ci/python[0-9.]+$")


def is_expired_keyed_cache(version: dict, cutoff: datetime) -> bool:
    tags = version.get("metadata", {}).get("container", {}).get("tags", [])
    if not tags or not all(KEYED_CACHE_TAG.match(tag) for tag in tags):
        return False
    return _parse_time(version["created_at"]) < cutoff


def find_expired_versions(versions: Iterable[dict], cutoff: datetime) -> list[dict]:
    return [version for version in versions if is_expired_keyed_cache(version, cutoff)]


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _gh_api_list(path: str) -> list[dict]:
    output = subprocess.run(
        ["gh", "api", "--paginate", path, "--jq", ".[]"], check=True, capture_output=True, text=True
    ).stdout
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _gh_api_page(path: str, page: int) -> list[dict]:
    output = subprocess.run(
        ["gh", "api", f"{path}?per_page=100&page={page}"], check=True, capture_output=True, text=True
    ).stdout
    return json.loads(output)


def get_versions_created_since(fetch_page: Callable[[int], list[dict]], since: datetime) -> list[dict]:
    """
    Return the versions created at or after ``since``.

    The API lists versions newest first, and a CI image package holds tens of thousands of them
    (every cache push adds manifests), so paging stops at the first page that reaches past ``since``.
    """
    versions: list[dict] = []
    page = 1
    while batch := fetch_page(page):
        versions.extend(version for version in batch if _parse_time(version["created_at"]) >= since)
        if _parse_time(batch[-1]["created_at"]) < since:
            break
        page += 1
    return versions


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--days", type=int, default=30, help="Keep keyed cache pushed within this many days")
    parser.add_argument(
        "--scan-days",
        type=int,
        default=7,
        help="Look this many days past the cutoff; anything older was deleted by earlier daily runs",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")
    args = parser.parse_args()

    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/", 1)
    cutoff = datetime.now(UTC) - timedelta(days=args.days)
    scan_since = cutoff - timedelta(days=args.scan_days)
    package_pattern = get_ci_image_package_pattern(repo)
    packages = [
        package["name"]
        for package in _gh_api_list(f"/orgs/{owner}/packages?package_type=container&per_page=100")
        if package_pattern.match(package["name"])
    ]
    deleted = 0
    for package in sorted(packages):
        versions_path = f"/orgs/{owner}/packages/container/{quote(package, safe='')}/versions"
        versions = get_versions_created_since(functools.partial(_gh_api_page, versions_path), scan_since)
        for version in find_expired_versions(versions, cutoff):
            tags = ", ".join(version["metadata"]["container"]["tags"])
            print(
                f"{'Would delete' if args.dry_run else 'Deleting'} {package}:{tags} ({version['updated_at']})"
            )
            if not args.dry_run:
                subprocess.run(
                    ["gh", "api", "-X", "DELETE", f"{versions_path}/{version['id']}"],
                    check=True,
                    capture_output=True,
                )
            deleted += 1
    print(f"{deleted} keyed cache version(s) older than {args.days} days in {len(packages)} package(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
