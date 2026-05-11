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
#
# Verify a packaged Apache Airflow Helm chart `.tgz`:
#
#   * Top-level entries match an explicit expected list (no IDE files,
#     workspace metadata, dev hook configs, or test sources sneak into the
#     published package).
#   * `helm lint` passes against the packaged tarball.
#
# Used by the `tests-helm-release` job in `.github/workflows/helm-tests.yml`
# right after `breeze release-management prepare-helm-chart-package` has
# produced the chart `.tgz`. Can also be run locally:
#
#     uv run scripts/ci/verify_helm_chart_package.py dist/airflow-1.22.0.tgz
#
# The expected top-level set is defined inline below; update it deliberately
# when a chart-level file is genuinely added or removed. Anything ignored via
# `chart/.helmignore` should not appear here.
#
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

# Files / directories expected at the top level of the packaged chart, i.e.
# inside the `airflow/` directory of `airflow-<version>.tgz`. Sorted for
# deterministic diffs. If you add or remove a file at the chart root, update
# this list along with `chart/.helmignore` (which controls what `helm package`
# bundles) so the two stay in sync.
EXPECTED_TOP_LEVEL: frozenset[str] = frozenset(
    {
        ".helmignore",
        "Chart.lock",
        "Chart.yaml",
        "INSTALL",
        "LICENSE",
        "NOTICE",
        "README.md",
        "RELEASE_NOTES.rst",
        "charts",
        "dockerfiles",
        "files",
        "reproducible_build.yaml",
        "templates",
        "values.schema.json",
        "values.yaml",
        "values_schema.schema.json",
    }
)


def _tarball_top_level(tarball: Path) -> tuple[str, set[str]]:
    """
    Read the chart `.tgz` and return (chart_dir_name, set_of_top_level_entries).

    `helm package` produces an archive whose entries are all rooted under a
    single directory named after the chart (e.g. `airflow/`). We strip that
    prefix to compare against EXPECTED_TOP_LEVEL.
    """
    chart_dirs: set[str] = set()
    top_level: set[str] = set()
    with tarfile.open(tarball, mode="r:gz") as tar:
        for member in tar.getmembers():
            parts = Path(member.name).parts
            if not parts:
                continue
            chart_dirs.add(parts[0])
            if len(parts) == 1:
                # The root chart dir entry itself; ignore it for the
                # top-level comparison.
                continue
            top_level.add(parts[1])

    if len(chart_dirs) != 1:
        raise SystemExit(
            f"FAIL: expected the tarball to contain a single top-level chart "
            f"directory, found {sorted(chart_dirs)} in {tarball}"
        )
    return next(iter(chart_dirs)), top_level


def _check_top_level(tarball: Path) -> tuple[str, bool]:
    chart_dir, actual = _tarball_top_level(tarball)
    extra = sorted(actual - EXPECTED_TOP_LEVEL)
    missing = sorted(EXPECTED_TOP_LEVEL - actual)
    ok = not extra and not missing

    print(f"chart-dir: {chart_dir}")
    print(f"top-level entries observed ({len(actual)}):")
    for name in sorted(actual):
        marker = "+" if name not in EXPECTED_TOP_LEVEL else " "
        print(f"  {marker} {name}")
    if extra:
        print()
        print(f"FAIL: unexpected top-level entries shipped in {tarball.name}:")
        for name in extra:
            print(f"  + {name}")
        print()
        print(
            "Either add the file to chart/.helmignore so it is not bundled, "
            "or — if it is genuinely a chart-consumer-relevant addition — "
            "extend EXPECTED_TOP_LEVEL in this script."
        )
    if missing:
        print()
        print(f"FAIL: expected top-level entries missing from {tarball.name}:")
        for name in missing:
            print(f"  - {name}")
        print()
        print(
            "If the file was genuinely removed from the chart on purpose, "
            "drop it from EXPECTED_TOP_LEVEL in this script."
        )
    return chart_dir, ok


def _run_helm_lint(tarball: Path) -> bool:
    helm = shutil.which("helm")
    if helm is None:
        print("FAIL: `helm` not found on PATH; cannot lint the chart.")
        return False
    print()
    print(f"$ helm lint {tarball}")
    proc = subprocess.run([helm, "lint", str(tarball)], check=False)
    return proc.returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify a packaged Apache Airflow Helm chart.",
    )
    parser.add_argument(
        "tarball",
        type=Path,
        help="Path to the packaged chart, e.g. dist/airflow-1.22.0.tgz",
    )
    args = parser.parse_args()

    tarball: Path = args.tarball
    if not tarball.is_file():
        print(f"FAIL: not a file: {tarball}")
        return 1

    print(f"Verifying packaged chart: {tarball}")
    print()
    _, top_level_ok = _check_top_level(tarball)
    lint_ok = _run_helm_lint(tarball)

    print()
    print("=== Summary ===")
    print(f"  top-level contents: {'OK' if top_level_ok else 'FAIL'}")
    print(f"  helm lint:          {'OK' if lint_ok else 'FAIL'}")
    return 0 if top_level_ok and lint_ok else 1


if __name__ == "__main__":
    sys.exit(main())
