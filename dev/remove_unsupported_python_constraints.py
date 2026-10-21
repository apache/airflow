#!/usr/bin/env python
#
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
# requires-python = ">=3.10"
# dependencies = [
#   "packaging>=24.0",
#   "rich-click>=1.7.0",
# ]
# ///
"""
Remove constraint files for Python versions that an Airflow release does not support.

Some ``constraints-<VERSION>`` tags carry ``constraints-3.X.txt`` files for a Python version the
release's ``requires-python`` excludes (for example 3.13 for Airflow 3.0.x). Those files pin
packages that have no wheels for that interpreter, so following the documented install command
ends in a source build failure instead of a clear "file not found".

For every affected tag the script commits the removal on top of the tagged commit and re-creates
the tag there. Release branches (``constraints-X-Y``) get the removal committed on their tip.
Nothing is pushed unless ``--push`` is passed; without ``--apply`` it only reports.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass

import rich_click as click
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version
from rich.console import Console
from rich.table import Table

console = Console()

METADATA_FILES = ["airflow-core/pyproject.toml", "pyproject.toml", "setup.cfg", "setup.py"]
CONSTRAINT_FILE_RE = re.compile(r"^constraints(?:-no-providers|-source-providers)?-(3\.\d+)\.txt$")
CLASSIFIER_RE = re.compile(r"Programming Language :: Python :: (3\.\d+)")
REQUIRES_PYTHON_RE = re.compile(r"""(?:requires-python|python_requires)\s*=\s*["']?([^"'\n]+)""")
# Probed to tell a capped requires-python (">=3.10,<3.14") from an open one ("~=3.6").
PROBE_PYTHON_VERSIONS = [Version(f"3.{minor}") for minor in range(6, 20)]


@dataclass
class Finding:
    ref: str
    airflow_version: str
    unsupported_pythons: list[str]
    files: list[str]
    reason: str


def run_git(*args: str, check: bool = True, env: dict[str, str] | None = None) -> str:
    return subprocess.run(["git", *args], check=check, capture_output=True, text=True, env=env).stdout


def read_file_at(ref: str, path: str) -> str | None:
    result = subprocess.run(["git", "show", f"{ref}:{path}"], check=False, capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else None


def is_minor_allowed(specifier: SpecifierSet, minor: Version) -> bool:
    """
    Check whether a Python minor version is allowed, judged by its latest patch release.

    A patch-level floor such as ``>=3.8.1`` still supports 3.8, while ``!=3.13`` is meant to exclude
    the whole minor even though PEP 440 only excludes 3.13.0 with it.
    """
    for spec in specifier:
        if spec.operator == "!=" and Version(spec.version.removesuffix(".*")).release[:2] == minor.release:
            return False
    return specifier.contains(Version(f"{minor}.99"), prereleases=True)


def find_supported_pythons(release_ref: str) -> tuple[set[str], str] | None:
    """Return the Python versions a release supports and where that was read from."""
    for path in METADATA_FILES:
        content = read_file_at(release_ref, path)
        if not content:
            continue
        classifiers = sorted(set(CLASSIFIER_RE.findall(content)), key=Version)
        if not classifiers:
            continue
        match = REQUIRES_PYTHON_RE.search(content)
        if not match:
            return set(classifiers), f"{path} classifiers"
        specifier = SpecifierSet(match.group(1).strip().rstrip(",").replace(" ", ""))
        allowed = [v for v in PROBE_PYTHON_VERSIONS if is_minor_allowed(specifier, v)]
        is_capped = bool(allowed) and any(
            not is_minor_allowed(specifier, v) for v in PROBE_PYTHON_VERSIONS if v > allowed[0]
        )
        if is_capped:
            # Classifiers lag behind requires-python (3.1.x supports 3.13 but does not list it),
            # so a capped specifier is authoritative.
            return {str(v) for v in allowed}, f"{path} requires-python {specifier}"
        # Open "~=3.6"-style specifiers say nothing about newer Pythons; fall back to classifiers.
        supported = {c for c in classifiers if is_minor_allowed(specifier, Version(c))}
        return supported, f"{path} classifiers within requires-python {specifier}"
    return None


def list_constraint_files(ref: str) -> dict[str, list[str]]:
    files_by_python: dict[str, list[str]] = {}
    for name in run_git("ls-tree", "--name-only", ref).splitlines():
        if match := CONSTRAINT_FILE_RE.match(name):
            files_by_python.setdefault(match.group(1), []).append(name)
    return files_by_python


def find_release_tags(include_prereleases: bool) -> list[tuple[str, str]]:
    """Return (constraints tag, Airflow release tag) pairs for 2.x and later."""
    pairs = []
    for tag in run_git("tag", "-l", "constraints-*").split():
        version_str = tag.removeprefix("constraints-")
        try:
            version = Version(version_str)
        except InvalidVersion:
            continue
        if version.major < 2 or (version.is_prerelease and not include_prereleases):
            continue
        if run_git("rev-parse", "-q", "--verify", f"refs/tags/{version_str}^{{commit}}", check=False):
            pairs.append((tag, version_str))
    return sorted(pairs, key=lambda pair: Version(pair[1]))


def find_branch_release(branch: str, release_tags: list[tuple[str, str]]) -> str | None:
    """Map ``constraints-X-Y`` to the newest final release of the X.Y line."""
    major_minor = branch.removeprefix("constraints-").replace("-", ".")
    candidates = [
        v
        for _, v in release_tags
        if re.fullmatch(rf"{re.escape(major_minor)}\.\d+", v) and not Version(v).is_prerelease
    ]
    return max(candidates, key=Version) if candidates else None


def analyze(ref: str, release_ref: str) -> Finding | None:
    supported = find_supported_pythons(release_ref)
    if supported is None:
        console.print(f"[yellow]Could not determine supported Pythons for {release_ref}, skipping {ref}")
        return None
    supported_pythons, reason = supported
    files_by_python = list_constraint_files(ref)
    unsupported = sorted((p for p in files_by_python if p not in supported_pythons), key=Version)
    if not unsupported:
        return None
    return Finding(
        ref=ref,
        airflow_version=release_ref,
        unsupported_pythons=unsupported,
        files=sorted(f for p in unsupported for f in files_by_python[p]),
        reason=reason,
    )


def build_commit_message(finding: Finding) -> str:
    pythons = ", ".join(finding.unsupported_pythons)
    return (
        f"Remove constraints for Python {pythons} unsupported by Airflow {finding.airflow_version}\n\n"
        f"Supported Python versions taken from {finding.reason}. The removed files pinned packages\n"
        f"without wheels for those interpreters, so installs failed while building them from source."
    )


def commit_removal(base: str, finding: Finding) -> str:
    """Commit the removal on top of ``base`` using a scratch index, leaving the worktree alone."""
    index_file = run_git("rev-parse", "--git-path", "remove-constraints-index").strip()
    env = {**os.environ, "GIT_INDEX_FILE": index_file}
    run_git("read-tree", f"{base}^{{tree}}", env=env)
    run_git("rm", "--cached", "--quiet", *finding.files, env=env)
    tree = run_git("write-tree", env=env).strip()
    return run_git("commit-tree", tree, "-p", base, "-m", build_commit_message(finding), env=env).strip()


@click.command()
@click.option("--remote", default="upstream", show_default=True, help="Remote holding the constraints refs.")
@click.option("--include-prereleases", is_flag=True, help="Also fix beta and rc constraints tags.")
@click.option("--apply", "do_apply", is_flag=True, help="Create the fix commits and move local refs.")
@click.option("--push", is_flag=True, help="Force-push the rewritten tags and branches (implies --apply).")
@click.option("--sign/--no-sign", default=True, show_default=True, help="GPG-sign re-created tags.")
def main(remote: str, include_prereleases: bool, do_apply: bool, push: bool, sign: bool):
    run_git(
        "fetch", remote, "--tags", "--force", f"+refs/heads/constraints-*:refs/remotes/{remote}/constraints-*"
    )
    release_tags = find_release_tags(include_prereleases)

    findings: list[tuple[Finding, str]] = []
    for constraints_tag, release in release_tags:
        if finding := analyze(constraints_tag, release):
            findings.append((finding, "tag"))
    branches = run_git("for-each-ref", "--format=%(refname:short)", f"refs/remotes/{remote}/constraints-*")
    for remote_branch in branches.split():
        branch = remote_branch.removeprefix(f"{remote}/")
        if not re.fullmatch(r"constraints-\d+-\d+", branch):
            continue
        if (branch_release := find_branch_release(branch, release_tags)) and (
            finding := analyze(remote_branch, branch_release)
        ):
            findings.append((finding, "branch"))

    table = Table(title="Constraints for unsupported Python versions")
    for column in ("Ref", "Kind", "Airflow", "Unsupported Python", "Supported per"):
        table.add_column(column)
    for finding, kind in findings:
        table.add_row(
            finding.ref, kind, finding.airflow_version, ", ".join(finding.unsupported_pythons), finding.reason
        )
    console.print(table)
    if not findings:
        return
    if not (do_apply or push):
        console.print("\nDry run. Re-run with [bold]--apply[/] to fix locally, [bold]--push[/] to publish.")
        return

    push_refspecs = []
    for finding, kind in findings:
        if kind == "tag":
            tag = finding.ref
            base = run_git("rev-parse", f"{tag}^{{commit}}").strip()
            message = run_git("tag", "-l", "--format=%(contents:subject)", tag).strip()
            new_commit = commit_removal(base, finding)
            run_git("tag", "-f", "-s" if sign else "-a", tag, new_commit, "-m", message)
            push_refspecs.append(f"+refs/tags/{tag}:refs/tags/{tag}")
        else:
            branch = finding.ref.removeprefix(f"{remote}/")
            base = run_git("rev-parse", finding.ref).strip()
            new_commit = commit_removal(base, finding)
            # Fast-forward on top of the branch tip, so no force is needed.
            push_refspecs.append(f"{new_commit}:refs/heads/{branch}")
        console.print(
            f"[green]{finding.ref}[/]: {base[:10]} -> {new_commit[:10]} (removed {len(finding.files)} files)"
        )

    if not push:
        console.print("\nLocal refs prepared. To publish, run:")
        # Plain print: rich wraps long lines, and pasting a wrapped command runs each line separately.
        print(" \\\n    ".join([f"git push {remote}", *push_refspecs]))
        return
    subprocess.run(["git", "push", remote, *push_refspecs], check=True)


if __name__ == "__main__":
    sys.exit(main())
