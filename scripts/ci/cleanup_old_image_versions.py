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
so a package such as ``airflow/main/ci/python3.11`` holds tens of thousands of them. This deletes
the untagged versions created before the cutoff, oldest first, up to a budget per run and with a
pause between deletions, so that a run stays far below the API rate limit of the workflow token
and the next run continues where this one stopped.

A version is only deleted when no tagged manifest of its package references it: the platform
manifests and attestations of a multi-platform image are untagged versions of their own and must
stay while the image tag that lists them exists.

Usage: cleanup_old_image_versions.py [--days N] [--max-deletions N] [--pause SECONDS] [--dry-run]
Environment: GITHUB_REPOSITORY (``owner/repo``), and a token for ``gh`` with ``packages: write``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
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
COMMIT_TAG = re.compile(r"^[0-9a-f]{7,40}$")


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

    The few long-lived tags have their listed manifests in ``keep``. The packages also hold
    thousands of old per-commit tags, too many to read the manifests of, so an untagged version
    pushed within PUSH_WINDOW of a tagged one, or of the page edges where the tagged one may sit
    on the next page, is taken for part of that image and kept: the manifests of a
    multi-platform image are pushed together with it.
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


def _registry_get(url: str, token: str, accept: str = "application/json") -> tuple[dict, str | None, str]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": accept})
    with urllib.request.urlopen(request, timeout=60) as response:
        return (
            json.load(response),
            response.headers.get("Docker-Content-Digest"),
            response.headers.get("Link", ""),
        )


def collect_keep_set(image: str) -> set[str]:
    """
    Return the digests of the long-lived (non-commit) tags of ``image`` and of the manifests they list.

    Read from the registry (the images are public) rather than from the packages API, which can
    only find the few tagged versions by listing every one of the tens of thousands of versions.
    """
    with urllib.request.urlopen(
        f"https://ghcr.io/token?scope=repository:{image}:pull", timeout=60
    ) as response:
        token = json.load(response)["token"]
    tags: list[str] = []
    url: str | None = f"https://ghcr.io/v2/{image}/tags/list?n=1000"
    while url:
        listing, _, link = _registry_get(url, token)
        tags.extend(listing.get("tags") or [])
        next_page = re.search(r"<([^>]+)>;\s*rel=\"next\"", link)
        url = f"https://ghcr.io{next_page.group(1)}" if next_page else None
    keep: set[str] = set()
    for tag in (tag for tag in tags if not COMMIT_TAG.match(tag)):
        manifest, digest, _ = _registry_get(
            f"https://ghcr.io/v2/{image}/manifests/{tag}", token, MANIFEST_MEDIA_TYPES
        )
        if digest:
            keep.add(digest)
        keep |= get_referenced_digests(manifest)
    return keep


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
    budget = args.max_deletions
    for package in packages:
        if budget <= 0:
            break
        versions_path = f"/orgs/{owner}/packages/container/{quote(package, safe='')}/versions"
        page = _get_last_page(versions_path)
        if not find_deletable_versions(_get_page(versions_path, page), set(), cutoff):
            continue
        keep = collect_keep_set(f"{owner}/{package}")
        # Versions are listed newest first, so walking from the last page goes oldest first, and
        # deleting from a page never moves the versions of the pages before it.
        while page >= 1 and budget > 0:
            batch = _get_page(versions_path, page)
            if not batch or _parse_time(batch[-1]["created_at"]) >= cutoff:
                break
            for version in find_deletable_versions(batch, keep, cutoff)[:budget]:
                print(
                    f"{'Would delete' if args.dry_run else 'Deleting'} "
                    f"{package}@{version['name']} ({version['created_at']})"
                )
                if not args.dry_run:
                    _gh_api(["-X", "DELETE", f"{versions_path}/{version['id']}"])
                    time.sleep(args.pause)
                budget -= 1
            page -= 1
    print(
        f"{args.max_deletions - budget} old untagged version(s) {'would be ' if args.dry_run else ''}deleted."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
