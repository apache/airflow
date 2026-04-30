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
"""Derive registry trigger inputs for publish-docs-to-s3.yml.

Implements the gating logic for astronomer/issues-airflow#1305.

Wave dispatches send INCLUDE_DOCS="all-providers apache-airflow-providers"
(see dev/breeze/src/airflow_breeze/commands/workflow_commands.py:188-195
and dev/README_RELEASE_PROVIDERS.md). When a meta-token is present and the
ref is a wave tag (providers/YYYY-MM-DD), this script derives the wave's
actual provider list from per-provider tags reachable from the wave ref
but not from the previous wave tag, so registry-build runs incrementally
on just those providers. Falls back to a full rebuild for non-wave refs
or when the derivation produces an empty list.

Tag-push ordering: README_RELEASE_PROVIDERS.md flows the wave-tag push
BEFORE provider RC tags. Today publish-docs-to-s3 is dispatched manually
AFTER all provider tags exist, so the derivation always has its inputs.
Any future auto-trigger on wave-tag push must address this race.

Inputs (env): INCLUDE_DOCS, REF, GITHUB_OUTPUT.
Outputs (written to GITHUB_OUTPUT):
  registry-providers   space-separated provider IDs, or "" for full build
  registry-full-build  "true" for full rebuild, "false" otherwise
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

WAVE_TAG_RE = re.compile(r"^providers/[0-9]{4}-[0-9]{2}-[0-9]{2}$")
WAVE_TAG_GLOB = "providers/[0-9]*-*-*"
PROVIDER_TAG_GLOB = "providers-*/*"
PROVIDER_TAG_RE = re.compile(r"^providers-(?P<id>.+?)/(?P<ver>.+)$")
RC_SUFFIX_RE = re.compile(r"rc[0-9]+$")

META_TOKENS = frozenset({"all", "all-providers", "apache-airflow-providers"})
NON_PROVIDER_TOKENS = frozenset(
    {
        "apache-airflow",
        "helm-chart",
        "docker-stack",
        "apache-airflow-ctl",
        "apache-airflow-task-sdk",
    }
)
PROVIDER_PREFIX = "apache-airflow-providers-"


def _git(*args: str) -> list[str]:
    result = subprocess.run(["git", *args], capture_output=True, text=True, check=True)
    return [line for line in result.stdout.splitlines() if line]


def previous_wave_tag(ref: str, wave_tags: list[str]) -> str | None:
    """Return the wave tag immediately preceding ref, or None.

    Filters input through WAVE_TAG_RE because the git glob `providers/[0-9]*-*-*`
    is broader than the strict `providers/YYYY-MM-DD` shape and matches
    unrelated tags (e.g. `providers/2.11/2026-02-16` for an Airflow 2.11
    release with a date suffix).
    """
    valid = [t for t in wave_tags if WAVE_TAG_RE.match(t)]
    try:
        idx = valid.index(ref)
    except ValueError:
        return None
    return valid[idx + 1] if idx + 1 < len(valid) else None


def wave_provider_ids(tags: list[str]) -> list[str]:
    """Provider IDs from per-provider tags, dropping rc tags. Sorted."""
    ids: set[str] = set()
    for tag in tags:
        if RC_SUFFIX_RE.search(tag):
            continue
        match = PROVIDER_TAG_RE.match(tag)
        if match:
            ids.add(match.group("id"))
    return sorted(ids)


def parse_explicit_packages(include_docs: str) -> list[str]:
    """Per-package incremental path -- INCLUDE_DOCS without meta-tokens."""
    providers: list[str] = []
    seen: set[str] = set()
    for token in include_docs.split():
        if token in NON_PROVIDER_TOKENS or token in META_TOKENS:
            continue
        if token.startswith(PROVIDER_PREFIX):
            provider_id = token[len(PROVIDER_PREFIX) :]
        else:
            provider_id = token.replace(".", "-")
        if provider_id and provider_id not in seen:
            seen.add(provider_id)
            providers.append(provider_id)
    return providers


def derive(include_docs: str, ref: str, *, git_runner=_git) -> tuple[str, bool, str]:
    """Return (registry_providers, full_build, log_message).

    Pure function -- pass git_runner=fake for tests.
    """
    tokens = include_docs.split()
    has_meta = any(t in META_TOKENS for t in tokens)

    if not has_meta:
        providers = parse_explicit_packages(include_docs)
        return " ".join(providers), False, ""

    if not WAVE_TAG_RE.match(ref):
        return "", True, f"REF={ref} is not a wave-tag pattern; using full rebuild for meta-token dispatch."

    wave_tags = git_runner("tag", "--list", WAVE_TAG_GLOB, "--sort=-creatordate")
    prev = previous_wave_tag(ref, wave_tags)
    if not prev:
        return (
            "",
            True,
            f"::warning::No predecessor wave tag found before {ref}; falling back to full rebuild.",
        )

    raw_tags = git_runner("tag", "--merged", ref, "--no-merged", prev, "--list", PROVIDER_TAG_GLOB)
    providers = wave_provider_ids(raw_tags)
    if not providers:
        return (
            "",
            True,
            f"::warning::Wave ref {ref} has no new per-provider tags since {prev}; falling back to full rebuild.",
        )

    joined = " ".join(providers)
    return joined, False, f"Derived wave providers ({prev} -> {ref}): {joined}"


def main() -> int:
    include_docs = os.environ.get("INCLUDE_DOCS", "")
    ref = os.environ.get("REF", "")
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        print("GITHUB_OUTPUT not set", file=sys.stderr)
        return 1

    registry_providers, full_build, log = derive(include_docs, ref)
    if log:
        print(log)
    with open(output_path, "a", encoding="utf-8") as fh:
        fh.write(f"registry-providers={registry_providers}\n")
        fh.write(f"registry-full-build={'true' if full_build else 'false'}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
