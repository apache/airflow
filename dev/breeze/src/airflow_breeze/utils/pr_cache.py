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
"""File-based JSON caching for PR triage data."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


class CacheStore:
    """Generic file-based JSON cache keyed by github_repository and a cache name.

    Consolidates the repeated pattern of cache-dir creation, get, and save.
    """

    def __init__(self, cache_name: str, *, ttl_seconds: int = 0):
        self._cache_name = cache_name
        self._ttl_seconds = ttl_seconds

    def cache_dir(self, github_repository: str) -> Path:
        """Return (and create) the cache directory for the given repository."""
        from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH

        safe_name = github_repository.replace("/", "_")
        cache_dir = Path(BUILD_CACHE_PATH) / self._cache_name / safe_name
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    def _file(self, github_repository: str, key: str) -> Path:
        return self.cache_dir(github_repository) / f"{key}.json"

    def get(self, github_repository: str, key: str, *, match: dict[str, str] | None = None) -> Any:
        """Load cached data. If *match* is given, each key/value must match the stored data."""
        cache_file = self._file(github_repository, key)
        if not cache_file.exists():
            return None
        try:
            data = json.loads(cache_file.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if self._ttl_seconds:
            if time.time() - data.get("cached_at", 0) >= self._ttl_seconds:
                return None
        if match:
            for k, v in match.items():
                if data.get(k) != v:
                    return None
        return data

    def save(self, github_repository: str, key: str, data: dict) -> None:
        """Save *data* as JSON. Automatically adds ``cached_at`` when TTL is configured."""
        if self._ttl_seconds:
            data = {**data, "cached_at": time.time()}
        self._file(github_repository, key).write_text(json.dumps(data, indent=2))


# Concrete cache stores — one per domain
review_cache = CacheStore("review_cache")
classification_cache = CacheStore("classification_cache")
triage_cache = CacheStore("triage_cache")
status_cache = CacheStore("status_cache", ttl_seconds=4 * 3600)
stats_interaction_cache = CacheStore("stats_interaction_cache")


# Convenience functions for common cache operations
def get_cached_review(github_repository: str, pr_number: int, head_sha: str) -> dict | None:
    data = review_cache.get(github_repository, f"pr_{pr_number}", match={"head_sha": head_sha})
    return data.get("review") if data else None


def save_review_cache(github_repository: str, pr_number: int, head_sha: str, review: dict) -> None:
    review_cache.save(github_repository, f"pr_{pr_number}", {"head_sha": head_sha, "review": review})


def get_cached_classification(
    github_repository: str, pr_number: int, head_sha: str, viewer_login: str
) -> tuple[str | None, str | None]:
    data = classification_cache.get(
        github_repository, f"pr_{pr_number}", match={"head_sha": head_sha, "viewer": viewer_login}
    )
    if data is None:
        return None, None
    return data.get("classification"), data.get("action")


def save_classification_cache(
    github_repository: str,
    pr_number: int,
    head_sha: str,
    viewer_login: str,
    classification: str,
    action: str | None = None,
) -> None:
    entry: dict[str, str] = {"head_sha": head_sha, "viewer": viewer_login, "classification": classification}
    if action:
        entry["action"] = action
    classification_cache.save(github_repository, f"pr_{pr_number}", entry)


def get_cached_assessment(
    github_repository: str, pr_number: int, head_sha: str, checks_state: str = ""
) -> dict | None:
    match = {"head_sha": head_sha}
    if checks_state:
        match["checks_state"] = checks_state
    data = triage_cache.get(github_repository, f"pr_{pr_number}", match=match)
    return data.get("assessment") if data else None


def save_assessment_cache(
    github_repository: str, pr_number: int, head_sha: str, assessment: dict, checks_state: str = ""
) -> None:
    entry: dict[str, Any] = {"head_sha": head_sha, "assessment": assessment}
    if checks_state:
        entry["checks_state"] = checks_state
    triage_cache.save(github_repository, f"pr_{pr_number}", entry)


def get_cached_status(github_repository: str, cache_key: str) -> Any:
    data = status_cache.get(github_repository, cache_key)
    return data.get("payload") if data else None


def save_status_cache(github_repository: str, cache_key: str, payload: dict | list) -> None:
    status_cache.save(github_repository, cache_key, {"payload": payload})


# PR-keyed caches that store head_sha and should be validated on startup
_PR_CACHES: list[CacheStore] = [review_cache, classification_cache, triage_cache]


def scan_cached_pr_numbers(github_repository: str) -> dict[int, dict[str, str]]:
    """Scan all PR-keyed caches and return {pr_number: {cache_name: head_sha}} for each cached PR.

    Only considers files matching the ``pr_<number>.json`` pattern.
    """
    import re

    pr_re = re.compile(r"^pr_(\d+)\.json$")
    result: dict[int, dict[str, str]] = {}
    for store in _PR_CACHES:
        cache_dir = store.cache_dir(github_repository)
        if not cache_dir.exists():
            continue
        for path in cache_dir.iterdir():
            m = pr_re.match(path.name)
            if not m:
                continue
            pr_num = int(m.group(1))
            try:
                data = json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            sha = data.get("head_sha")
            if sha:
                result.setdefault(pr_num, {})[store._cache_name] = sha
    return result


def invalidate_stale_caches(github_repository: str, stale_prs: dict[int, str]) -> int:
    """Remove cache entries for PRs whose head_sha no longer matches.

    *stale_prs* maps ``{pr_number: current_head_sha}`` — any cached entry with a
    different ``head_sha`` is deleted.

    Returns the number of cache files removed.
    """
    removed = 0
    for pr_number, current_sha in stale_prs.items():
        key = f"pr_{pr_number}"
        for store in _PR_CACHES:
            cache_file = store._file(github_repository, key)
            if not cache_file.exists():
                continue
            try:
                data = json.loads(cache_file.read_text())
            except (json.JSONDecodeError, OSError):
                cache_file.unlink(missing_ok=True)
                removed += 1
                continue
            if data.get("head_sha") != current_sha:
                cache_file.unlink(missing_ok=True)
                removed += 1
    return removed
