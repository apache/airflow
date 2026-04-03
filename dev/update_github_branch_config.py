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
# /// script
# requires-python = ">=3.9"
# ///
"""
Update .github/ configuration files on ``main`` when a new major/minor
release branch (vX-Y-test) is created.

Adds the new branch to dependabot, milestone-tag-assistant, basic-tests,
ci-notification workflows, boring-cyborg auto-labelling, and the
README.md build status badge.

Usage::

    uv run dev/update_github_branch_config.py <NEW_MAJOR> <NEW_MINOR>

Example — after creating the v3-3-test branch::

    uv run dev/update_github_branch_config.py 3 3
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _read(path: Path) -> str:
    return path.read_text()


def _write(path: Path, content: str) -> None:
    path.write_text(content)


# ──────────────────────────────────────────────────────────────────────────────
# dependabot.yml
# ──────────────────────────────────────────────────────────────────────────────
def update_dependabot(new_branch: str, prev_branch: str, new_dash: str) -> None:
    path = REPO_ROOT / ".github" / "dependabot.yml"
    content = _read(path)

    if f"target-branch: {new_branch}" in content:
        print(f"dependabot.yml: {new_branch} already present, skipping.")
        return

    print(f"dependabot.yml: adding {new_branch} entries...")

    # 1. Add github-actions entry before the existing entry for prev_branch
    gh_actions_new_entry = f"""\
  - package-ecosystem: "github-actions"
    directory: "/"
    cooldown:
      default-days: 4
    schedule:
      # Check for updates to GitHub Actions every week
      interval: "weekly"
    target-branch: {new_branch}
    groups:
      github-actions-updates:
        patterns:
          - "*"

"""
    # Match the full existing block for prev_branch
    prev_gh_actions = f"""\
  - package-ecosystem: "github-actions"
    directory: "/"
    cooldown:
      default-days: 4
    schedule:
      # Check for updates to GitHub Actions every week
      interval: "weekly"
    target-branch: {prev_branch}
"""
    content = content.replace(prev_gh_actions, gh_actions_new_entry + prev_gh_actions, 1)

    # 2. Add pip + npm entries before the previous branch's comment block
    pip_npm_block = f"""\
  # Repeat dependency updates on {new_branch} branch as well
  - package-ecosystem: pip
    cooldown:
      default-days: 4
    directories:
      - /airflow-core
      - /airflow-ctl
      - /clients/python
      - /dev/breeze
      - /docker-tests
      - /kubernetes-tests
      - /helm-tests
      - /task-sdk
      - /
    schedule:
      interval: daily
    target-branch: {new_branch}
    groups:
      pip-dependency-updates:
        patterns:
          - "*"

  - package-ecosystem: npm
    cooldown:
      default-days: 4
    directories:
      - /airflow-core/src/airflow/ui
    schedule:
      interval: "weekly"
    target-branch: {new_branch}
    groups:
      {new_dash}-core-ui-package-updates:
        patterns:
          - "*"
        update-types:
          - "minor"
          - "patch"
    ignore:
      - dependency-name: "*"
        update-types: ["version-update:semver-major"]

  - package-ecosystem: npm
    cooldown:
      default-days: 4
    directories:
      - /airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui
    schedule:
      interval: "weekly"
    target-branch: {new_branch}
    groups:
      {new_dash}-auth-ui-package-updates:
        patterns:
          - "*"
        update-types:
          - "minor"
          - "patch"
    ignore:
      - dependency-name: "*"
        update-types: ["version-update:semver-major"]

"""
    prev_comment = f"  # Repeat dependency updates on {prev_branch} branch as well"
    content = content.replace(prev_comment, pip_npm_block + prev_comment, 1)

    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# milestone-tag-assistant.yml
# ──────────────────────────────────────────────────────────────────────────────
def update_milestone_tag_assistant(new_branch: str, prev_branch: str) -> None:
    path = REPO_ROOT / ".github" / "workflows" / "milestone-tag-assistant.yml"
    content = _read(path)

    if f"- {new_branch}" in content:
        print(f"milestone-tag-assistant.yml: {new_branch} already present, skipping.")
        return

    print(f"milestone-tag-assistant.yml: adding {new_branch}...")
    content = content.replace(
        f"      - {prev_branch}",
        f"      - {new_branch}\n      - {prev_branch}",
        1,
    )
    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# basic-tests.yml
# ──────────────────────────────────────────────────────────────────────────────
def update_basic_tests(
    new_branch: str,
    prev_branch: str,
    new_version: str,
    prev_version: str,
    new_dash: str,
    prev_dash: str,
) -> None:
    path = REPO_ROOT / ".github" / "workflows" / "basic-tests.yml"
    content = _read(path)

    print(f"basic-tests.yml: updating release-management dry-run versions to {new_version}...")

    content = content.replace(
        f"create-minor-branch --version-branch {prev_dash}",
        f"create-minor-branch --version-branch {new_dash}",
    )
    content = content.replace(
        f"--sync-branch {prev_branch}",
        f"--sync-branch {new_branch}",
    )

    # Update start-rc-process version and previous-version
    content = content.replace(
        f"start-rc-process --version {prev_version}.0rc1",
        f"start-rc-process --version {new_version}.0rc1",
    )
    content = re.sub(
        r"--previous-version \d+\.\d+\.0(\s+.*--task-sdk-version)",
        f"--previous-version {prev_version}.0\\1",
        content,
    )
    # Bump task-sdk version: increment minor
    content = re.sub(
        r"--task-sdk-version \d+\.\d+\.0rc1",
        f"--task-sdk-version 1.{int(new_version.split('.')[1]) - 1}.0rc1",
        content,
    )

    # Update start-release version
    content = re.sub(
        rf"start-release --version {re.escape(prev_version)}\.\d+",
        f"start-release --version {new_version}.7",
        content,
    )

    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# ci-notification.yml
# ──────────────────────────────────────────────────────────────────────────────
def update_ci_notification(new_branch: str, prev_branch: str) -> None:
    path = REPO_ROOT / ".github" / "workflows" / "ci-notification.yml"
    content = _read(path)

    print(f"ci-notification.yml: switching branch to {new_branch}...")
    content = content.replace(
        f'branch: ["{prev_branch}"]',
        f'branch: ["{new_branch}"]',
    )
    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# boring-cyborg.yml
# ──────────────────────────────────────────────────────────────────────────────
BORING_CYBORG_BACKPORT_BLOCK = """\
  # This should be copy of the "area:dev-tools" above minus contributing docs and some files that should
  # only make sense in main - it should be updated when we switch maintenance branch
  backport-to-{new_branch}:
    - scripts/**/*
    - dev/**/*
    - .github/**/*
    - Dockerfile.ci
    - yamllint-config.yml
    - .dockerignore
    - .hadolint.yaml
    - .pre-commit-config.yaml
    - .rat-excludes

"""


def update_boring_cyborg(new_branch: str, prev_branch: str) -> None:
    path = REPO_ROOT / ".github" / "boring-cyborg.yml"
    content = _read(path)

    if f"backport-to-{new_branch}:" in content:
        print(f"boring-cyborg.yml: backport-to-{new_branch} already present, skipping.")
        return

    print(f"boring-cyborg.yml: adding backport-to-{new_branch}...")
    new_block = BORING_CYBORG_BACKPORT_BLOCK.format(new_branch=new_branch)
    anchor = f"  backport-to-{prev_branch}:"
    # Find the comment block preceding the previous backport entry
    prev_comment = f"""\
  # This should be copy of the "area:dev-tools" above minus contributing docs and some files that should
  # only make sense in main - it should be updated when we switch maintenance branch
  backport-to-{prev_branch}:"""
    if prev_comment in content:
        content = content.replace(prev_comment, new_block + anchor, 1)
    else:
        content = content.replace(anchor, new_block + anchor, 1)

    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# README.md — build status badge
# ──────────────────────────────────────────────────────────────────────────────
def update_readme_badge(new_branch: str, prev_branch: str, new_version: str, prev_version: str) -> None:
    path = REPO_ROOT / "README.md"
    content = _read(path)

    old_badge = (
        f"[![GitHub Build {prev_version}"
        f"](https://github.com/apache/airflow/actions/workflows/ci-amd-arm.yml/badge.svg"
        f"?branch={prev_branch})](https://github.com/apache/airflow/actions)"
    )
    new_badge = (
        f"[![GitHub Build {new_version}"
        f"](https://github.com/apache/airflow/actions/workflows/ci-amd-arm.yml/badge.svg"
        f"?branch={new_branch})](https://github.com/apache/airflow/actions)"
    )

    if new_branch in content:
        print(f"README.md: {new_branch} badge already present, skipping.")
        return

    if old_badge not in content:
        print(f"README.md: could not find existing {prev_branch} badge, skipping.")
        return

    print(f"README.md: updating 3.x build status badge to {new_branch}...")
    content = content.replace(old_badge, new_badge)
    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# dev/README_RELEASE_AIRFLOW.md
# ──────────────────────────────────────────────────────────────────────────────
def update_release_readme(new_branch: str, prev_branch: str) -> None:
    path = REPO_ROOT / "dev" / "README_RELEASE_AIRFLOW.md"
    content = _read(path)

    print(f"README_RELEASE_AIRFLOW.md: updating apache/{prev_branch} references to apache/{new_branch}...")
    content = content.replace(f"apache/{prev_branch}", f"apache/{new_branch}")
    _write(path, content)


# ──────────────────────────────────────────────────────────────────────────────
# main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <NEW_MAJOR> <NEW_MINOR>")
        print(f"Example: {sys.argv[0]} 3 3   (for v3-3-test branch)")
        sys.exit(1)

    new_major = int(sys.argv[1])
    new_minor = int(sys.argv[2])
    prev_minor = new_minor - 1

    new_branch = f"v{new_major}-{new_minor}-test"
    prev_branch = f"v{new_major}-{prev_minor}-test"
    new_version = f"{new_major}.{new_minor}"
    prev_version = f"{new_major}.{prev_minor}"
    new_dash = f"{new_major}-{new_minor}"
    prev_dash = f"{new_major}-{prev_minor}"

    print(f"Updating .github/ configs: adding {new_branch} (previous: {prev_branch})")
    print(f"Repository root: {REPO_ROOT}")
    print()

    update_dependabot(new_branch, prev_branch, new_dash)
    update_milestone_tag_assistant(new_branch, prev_branch)
    update_basic_tests(new_branch, prev_branch, new_version, prev_version, new_dash, prev_dash)
    update_ci_notification(new_branch, prev_branch)
    update_boring_cyborg(new_branch, prev_branch)
    update_readme_badge(new_branch, prev_branch, new_version, prev_version)
    update_release_readme(new_branch, prev_branch)

    print()
    print("Done! Please review the changes with 'git diff' before committing.")
    print()
    print("Additional manual steps:")
    print(f"  - Create the 'backport-to-{new_branch}' label:")
    print(
        f"      gh label create 'backport-to-{new_branch}' --repo apache/airflow"
        f" --description 'Backport to {new_branch}' --color 0e8a16"
    )
    print("  - Update test references in dev/breeze/tests/test_set_milestone.py if needed")
    print("  - Update dev/README_AIRFLOW3_DEV.md references if needed")


if __name__ == "__main__":
    main()
