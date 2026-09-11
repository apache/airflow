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
Keep the Go toolchain version in sync across every file that pins it.

``golang.org/x/net`` (and other ``golang.org/x`` modules) periodically raise
their minimum Go version. When that happens a dependency bump changes the
``go`` directive in ``go-sdk/go.mod`` but leaves the *other* places that pin
the toolchain untouched — and CI then fails deep inside static checks and the
Go SDK e2e bundle build with ``go.mod requires go >= <new>`` (this is exactly
what happened in PR #69214). Those pins have no cross-file include, so nothing
catches the drift until CI does.

The single source of truth is the ``go`` directive in ``go-sdk/go.mod``. Every
other site below must agree with it at ``major.minor`` granularity (the minor
version is what selects the toolchain and the builder image; the go.mod patch
suffix is not meaningful for those):

- ``go-sdk/go.mod``                             -> ``go <maj.min[.patch]>``  (SOURCE OF TRUTH)
- ``kubernetes-tests/lang_sdk/go_example/go.mod`` -> ``go <maj.min[.patch]>``  (resolves the SDK via ``replace``)
- ``.github/workflows/ci-amd.yml``              -> Setup Go ``go-version: <maj.min>``
- ``.github/workflows/ci-arm.yml``              -> Setup Go ``go-version: <maj.min>``
- ``airflow-e2e-tests/tests/airflow_e2e_tests/constants.py`` -> ``GO_BUILDER_IMAGE`` default ``golang:<maj.min>-alpine``
- ``dev/breeze/src/airflow_breeze/commands/kubernetes_commands.py`` -> ``LANG_SDK_GO_BUILDER_IMAGE`` default ``golang:<maj.min>-alpine``
- ``go-sdk/.pre-commit-config.yaml``            -> ``default_language_version.golang`` (the toolchain prek uses for the SDK's ``language: golang`` hooks — this is the pin that broke static checks in #69214, not the setup-go one)
- ``.pre-commit-config.yaml``                   -> ``default_language_version.golang`` (top-level default for any ``language: golang`` hook)

When they disagree, bump the drifting sites to the source-of-truth minor
version. If you add a new place that pins the Go version, register it here too.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_go_version_in_sync.py

Exits 0 if every site agrees with the source of truth, 1 otherwise.
"""

from __future__ import annotations

import dataclasses
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

# The ``go`` directive: ``go 1.25`` or ``go 1.25.0``.
GO_MOD_DIRECTIVE = re.compile(r"^go\s+(\d+\.\d+(?:\.\d+)?)\s*$", re.MULTILINE)
# Setup Go step: ``go-version: 1.25`` (optionally quoted).
WORKFLOW_GO_VERSION = re.compile(r"^\s*go-version:\s*[\"']?(\d+\.\d+(?:\.\d+)?)[\"']?\s*$", re.MULTILINE)
# Builder image default: ``golang:1.25-alpine``.
GOLANG_BUILDER_IMAGE = re.compile(r"golang:(\d+\.\d+(?:\.\d+)?)-alpine")
# prek ``default_language_version`` entry: ``  golang: 1.25.0``.
PREK_GOLANG_VERSION = re.compile(r"^\s*golang:\s*(\d+\.\d+(?:\.\d+)?)\s*$", re.MULTILINE)


@dataclasses.dataclass
class VersionSite:
    """A single location that pins the Go toolchain version."""

    label: str
    path: pathlib.Path
    pattern: re.Pattern[str]
    is_source_of_truth: bool = False

    def extract(self) -> str | None:
        """Return the captured version string, or ``None`` if the pattern is missing."""
        if not self.path.exists():
            return None
        if m := self.pattern.search(self.path.read_text()):
            return m.group(1)
        return None


def major_minor(version: str) -> str:
    """Reduce ``1.25`` / ``1.25.0`` to its ``major.minor`` (``1.25``)."""
    parts = version.split(".")
    return ".".join(parts[:2])


def build_sites(repo_root: pathlib.Path) -> list[VersionSite]:
    """Build the list of Go-version pin sites, resolved against ``repo_root``."""
    workflows = repo_root / ".github" / "workflows"
    return [
        VersionSite(
            label="go-sdk/go.mod  (go directive)",
            path=repo_root / "go-sdk" / "go.mod",
            pattern=GO_MOD_DIRECTIVE,
            is_source_of_truth=True,
        ),
        VersionSite(
            label="kubernetes-tests/lang_sdk/go_example/go.mod  (go directive)",
            path=repo_root / "kubernetes-tests" / "lang_sdk" / "go_example" / "go.mod",
            pattern=GO_MOD_DIRECTIVE,
        ),
        VersionSite(
            label=".github/workflows/ci-amd.yml  (Setup Go go-version)",
            path=workflows / "ci-amd.yml",
            pattern=WORKFLOW_GO_VERSION,
        ),
        VersionSite(
            label=".github/workflows/ci-arm.yml  (Setup Go go-version)",
            path=workflows / "ci-arm.yml",
            pattern=WORKFLOW_GO_VERSION,
        ),
        VersionSite(
            label="airflow-e2e-tests/.../constants.py  (GO_BUILDER_IMAGE)",
            path=repo_root / "airflow-e2e-tests" / "tests" / "airflow_e2e_tests" / "constants.py",
            pattern=GOLANG_BUILDER_IMAGE,
        ),
        VersionSite(
            label="dev/breeze/.../kubernetes_commands.py  (LANG_SDK_GO_BUILDER_IMAGE)",
            path=repo_root
            / "dev"
            / "breeze"
            / "src"
            / "airflow_breeze"
            / "commands"
            / "kubernetes_commands.py",
            pattern=GOLANG_BUILDER_IMAGE,
        ),
        VersionSite(
            label="go-sdk/.pre-commit-config.yaml  (default_language_version.golang)",
            path=repo_root / "go-sdk" / ".pre-commit-config.yaml",
            pattern=PREK_GOLANG_VERSION,
        ),
        VersionSite(
            label=".pre-commit-config.yaml  (default_language_version.golang)",
            path=repo_root / ".pre-commit-config.yaml",
            pattern=PREK_GOLANG_VERSION,
        ),
    ]


def check_sync(sites: list[VersionSite], repo_root: pathlib.Path) -> tuple[int, str]:
    """Compare every site against the source of truth. Returns ``(exit_code, report)``."""
    results = [(site, site.extract()) for site in sites]

    if missing := [site for site, version in results if version is None]:
        lines = [f"ERROR: Go version pin not found in {site.path.relative_to(repo_root)}" for site in missing]
        lines += [f"       (expected pattern {site.pattern.pattern!r})" for site in missing]
        return 1, "\n".join(lines)

    source = next(site for site, _ in results if site.is_source_of_truth)
    expected = major_minor(source.extract())  # type: ignore[arg-type]

    drifted = [(site, version) for site, version in results if major_minor(version) != expected]  # type: ignore[arg-type]
    if not drifted:
        return 0, f"OK: Go version is consistently {expected} across all {len(sites)} pin sites."

    col = max(len(site.label) for site, _ in results)
    lines = [
        f"ERROR: Go version drifted from the source of truth ({source.label} = {expected}).",
        "",
        f"  {'PIN SITE':<{col}}  VERSION",
    ]
    for site, version in results:
        marker = (
            "  <- source of truth"
            if site.is_source_of_truth
            else (
                "  <- DRIFT" if major_minor(version) != expected else ""  # type: ignore[arg-type]
            )
        )
        lines.append(f"  {site.label:<{col}}  {version}{marker}")
    lines += [
        "",
        f"Bump the drifting site(s) to {expected} to match {source.label},",
        "or, if you intentionally changed the source of truth, update the other pins to match.",
        "After changing a go.mod, run its `go mod tidy` so go.sum stays consistent.",
    ]
    return 1, "\n".join(lines)


def main() -> int:
    exit_code, report = check_sync(build_sites(REPO_ROOT), REPO_ROOT)
    print(report)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
