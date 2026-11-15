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
Slowly delete old untagged versions of the CI and PROD image packages.

Every image or cache push leaves the manifest it replaced behind as an untagged package version,
so a package such as ``airflow/main/ci/python3.11`` holds tens of thousands of them. Each run
takes one package, in turn by run number, and deletes its untagged versions created before the
cutoff, oldest first, up to a budget and with a pause between deletions, so that a run stays far
below the API rate limit of the workflow token and a later run continues where this one stopped.

A version is only deleted when no tag of its package references it: the platform manifests and
attestations of a multi-platform image are untagged versions of their own and must stay while the
tag that lists them exists. The references are read from the registry for every tag, and an
untagged version pushed within PUSH_WINDOW of a tagged one is kept as well.

Usage: cleanup_old_image_versions.py [--days N] [--max-deletions N] [--pause SECONDS] [--dry-run]
Environment: GITHUB_REPOSITORY (``owner/repo``), GITHUB_RUN_NUMBER (picks the package), and a
token for ``gh`` with ``packages: write``.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

MANIFEST_MEDIA_TYPES = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)
LINK_LAST_PAGE = re.compile(r'[?&]page=(\d+)>; rel="last"')
PUSH_WINDOW = timedelta(minutes=5)


def get_image_package_pattern(repo: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(repo)}/[^/]+/(ci|prod)/python[0-9.]+$")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_referenced_digests(manifest: dict) -> set[str]:
    return {entry["digest"] for entry in manifest.get("manifests", []) if "digest" in entry}


def _get_tags(version: dict) -> list[str]:
    return version.get("metadata", {}).get("container", {}).get("tags") or []


def find_deletable_versions(versions: list[dict], keep: set[str], cutoff: datetime) -> list[dict]:
    """
    Return the untagged versions of one page that are safe to delete.

    ``keep`` holds every manifest a tag references. As a second guard, an untagged version pushed
    within PUSH_WINDOW of a tagged one, or of the page edges where a tagged one may sit on the
    next page, is kept: the manifests of a multi-platform image are pushed together with it.
    """
    if not versions:
        return []
    anchors = [_parse_time(v["created_at"]) for v in versions if _get_tags(v)]
    anchors += [_parse_time(versions[0]["created_at"]), _parse_time(versions[-1]["created_at"])]
    return [
        version
        for version in versions
        if not _get_tags(version)
        and version["name"] not in keep
        and (created := _parse_time(version["created_at"])) < cutoff
        and all(abs(created - anchor) > PUSH_WINDOW for anchor in anchors)
    ]


def _gh_api(args: list[str]) -> str:
    return subprocess.run(["gh", "api", *args], check=True, capture_output=True, text=True).stdout


def _get_last_page(versions_path: str) -> int:
    headers = _gh_api(["--include", "--method", "GET", f"{versions_path}?per_page=100&page=1", "--jq", "."])
    match = LINK_LAST_PAGE.search(headers)
    return int(match.group(1)) if match else 1


def _get_page(versions_path: str, page: int) -> list[dict]:
    return json.loads(_gh_api([f"{versions_path}?per_page=100&page={page}"]))


class _Registry:
    """Anonymous, read-only access to the public images on ghcr.io."""

    def __init__(self, image: str):
        self.image = image
        self.token = self._get_token()

    def _get_token(self) -> str:
        url = f"https://ghcr.io/token?scope=repository:{self.image}:pull"
        with urllib.request.urlopen(url, timeout=60) as response:
            return json.load(response)["token"]

    def get(self, path: str, accept: str = "application/json") -> tuple[dict, str | None, str]:
        for attempt in range(2):
            request = urllib.request.Request(
                f"https://ghcr.io{path}", headers={"Authorization": f"Bearer {self.token}", "Accept": accept}
            )
            try:
                with urllib.request.urlopen(request, timeout=60) as response:
                    return (
                        json.load(response),
                        response.headers.get("Docker-Content-Digest"),
                        response.headers.get("Link", ""),
                    )
            except urllib.error.HTTPError as error:
                if error.code != 401 or attempt:
                    raise
                self.token = self._get_token()
        raise AssertionError("unreachable")


def collect_keep_set(image: str) -> set[str]:
    """
    Return the digests of every tagged manifest of ``image`` and of the manifests they list.

    Read from the registry (the images are public) rather than from the packages API, which can
    only find the tagged versions by listing every one of the tens of thousands of versions, and
    whose rate limit the deletions need.
    """
    registry = _Registry(image)
    tags: list[str] = []
    path: str | None = f"/v2/{image}/tags/list?n=1000"
    while path:
        listing, _, link = registry.get(path)
        tags.extend(listing.get("tags") or [])
        next_page = re.search(r"<([^>]+)>;\s*rel=\"next\"", link)
        path = next_page.group(1) if next_page else None

    def read_tag(tag: str) -> set[str]:
        manifest, digest, _ = registry.get(f"/v2/{image}/manifests/{tag}", MANIFEST_MEDIA_TYPES)
        return get_referenced_digests(manifest) | ({digest} if digest else set())

    keep: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as executor:
        for digests in executor.map(read_tag, tags):
            keep |= digests
    print(f"{image}: {len(tags)} tag(s) reference {len(keep)} manifest(s)")
    return keep


def select_versions_to_delete(
    fetch_page: Callable[[int], list[dict]],
    last_page: int,
    load_keep: Callable[[], set[str]],
    cutoff: datetime,
    budget: int,
) -> list[dict]:
    """
    Walk the pages from the last, oldest one, and return up to ``budget`` versions to delete.

    Versions are listed newest first, so deleting from a page never moves the versions of the
    pages before it, and the selection can be made before deleting. The tag references are only
    loaded once a page has a candidate, and a page with none does not stop the walk.
    """
    selected: list[dict] = []
    keep: set[str] | None = None
    for page in range(last_page, 0, -1):
        if len(selected) >= budget:
            break
        batch = fetch_page(page)
        if not batch or _parse_time(batch[-1]["created_at"]) >= cutoff:
            break
        if keep is None:
            if not find_deletable_versions(batch, set(), cutoff):
                continue
            keep = load_keep()
        selected.extend(find_deletable_versions(batch, keep, cutoff)[: budget - len(selected)])
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--days", type=int, default=30, help="Keep versions created within this many days")
    parser.add_argument("--max-deletions", type=int, default=300, help="Stop after this many deletions")
    parser.add_argument("--pause", type=float, default=1.0, help="Seconds to wait after each deletion")
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")
    args = parser.parse_args()

    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/", 1)
    cutoff = datetime.now(UTC) - timedelta(days=args.days)
    package_pattern = get_image_package_pattern(repo)
    package_listing = _gh_api(
        ["--paginate", f"/orgs/{owner}/packages?package_type=container&per_page=100", "--jq", ".[].name"]
    )
    packages = sorted(name for name in package_listing.split() if package_pattern.match(name))
    if not packages:
        print("No image packages found.")
        return 0
    package = packages[int(os.environ.get("GITHUB_RUN_NUMBER", "0")) % len(packages)]
    versions_path = f"/orgs/{owner}/packages/container/{quote(package, safe='')}/versions"
    selected = select_versions_to_delete(
        functools.partial(_get_page, versions_path),
        _get_last_page(versions_path),
        functools.partial(collect_keep_set, f"{owner}/{package}"),
        cutoff,
        args.max_deletions,
    )
    for version in selected:
        print(
            f"{'Would delete' if args.dry_run else 'Deleting'} "
            f"{package}@{version['name']} ({version['created_at']})"
        )
        if not args.dry_run:
            _gh_api(["-X", "DELETE", f"{versions_path}/{version['id']}"])
            time.sleep(args.pause)
    print(f"{package}: {len(selected)} old untagged version(s) {'would be ' if args.dry_run else ''}deleted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
