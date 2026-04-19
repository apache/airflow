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
from __future__ import annotations

import ast
import io
import json
import os
import re
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import TextIO

import requests
from click import Choice
from in_container_utils import AIRFLOW_DIST_PATH, AIRFLOW_ROOT_PATH, click, console, run_command

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

DEFAULT_BRANCH = os.environ.get("DEFAULT_BRANCH", "main")
PYTHON_VERSION = os.environ.get("PYTHON_MAJOR_MINOR_VERSION", "3.10")
GENERATED_PROVIDER_DEPENDENCIES_FILE = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"

ALL_PROVIDER_DEPENDENCIES = json.loads(GENERATED_PROVIDER_DEPENDENCIES_FILE.read_text())


def _read_version_from_pyproject(pyproject_path: Path) -> str:
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    version = data.get("project", {}).get("version")
    if not version:
        raise RuntimeError(f"Couldn't find project.version in {pyproject_path}")
    return str(version)


def _read_dynamic_version_from_init(init_path: Path) -> str:
    ast_obj = ast.parse(init_path.read_text())
    for node in ast_obj.body:
        if isinstance(node, ast.Assign) and node.targets[0].id == "__version__":  # type: ignore[attr-defined]
            return ast.literal_eval(node.value)
    raise RuntimeError(f"Couldn't find __version__ in {init_path}")


AIRFLOW_VERSION = _read_version_from_pyproject(AIRFLOW_ROOT_PATH / "pyproject.toml")
AIRFLOW_CORE_VERSION = _read_version_from_pyproject(AIRFLOW_ROOT_PATH / "airflow-core" / "pyproject.toml")
# task-sdk pyproject.toml declares [tool.hatch.version] with a dynamic source — read it there.
AIRFLOW_TASK_SDK_VERSION = _read_dynamic_version_from_init(
    AIRFLOW_ROOT_PATH / "task-sdk" / "src" / "airflow" / "sdk" / "__init__.py"
)

now = datetime.now().isoformat()

NO_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via `uv sync --resolution highest` for the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install just the 'bare' 'apache-airflow' package build from the HEAD of
# the branch, without installing any of the providers.
#
# Those constraints represent the "newest" dependencies airflow could use, if providers did not limit
# Airflow in any way.
#
"""

SOURCE_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via `uv sync --resolution highest for the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version of both
# 'apache-airflow' package and all available community provider distributions.
#
# Those constraints represent the dependencies that are used by all pull requests when they are build in CI.
# They represent "latest" and greatest set of constraints that HEAD of the "apache-airflow" package should
# Install with "HEAD" of providers. Those are the only constraints that are used by our CI builds.
#
"""

PYPI_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via `uv pip install --resolution highest` for the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version for 'apache-airflow' but installs
# the providers from PIP-released packages at the moment of the constraint generation.
#
# Those constraints are actually those that regular users use to install released version of Airflow.
# We also use those constraints after "apache-airflow" is released and the constraints are tagged with
# "constraints-X.Y.Z" tag to build the production image for that version.
#
# This constraints file is meant to be used only in the "apache-airflow" installation command and not
# in all subsequent pip commands. By using a constraints.txt file, we ensure that solely the Airflow
# installation step is reproducible. Subsequent pip commands may install packages that would have
# been incompatible with the constraints used in Airflow reproducible installation step. Finally, pip
# commands that might change the installed version of apache-airflow should include "apache-airflow==X.Y.Z"
# in the list of install targets to prevent Airflow accidental upgrade or downgrade.
#
# Typical installation process of airflow for Python {PYTHON_VERSION} is (with random selection of extras and custom
# dependencies added), usually consists of two steps:
#
# 1. Reproducible installation of airflow with selected providers (note constraints are used):
#
# pip install "apache-airflow[celery,cncf.kubernetes,google,amazon,snowflake]==X.Y.Z" \\
#     --constraint \\
#    "https://raw.githubusercontent.com/apache/airflow/constraints-X.Y.Z/constraints-{PYTHON_VERSION}.txt"
#
# 2. Installing own dependencies that are potentially not matching the constraints (note constraints are not
#    used, and apache-airflow==X.Y.Z is used to make sure there is no accidental airflow upgrade/downgrade.
#
# pip install "apache-airflow==X.Y.Z" "snowflake-connector-python[pandas]=N.M.O"
#
"""

BUILD_CONSTRAINTS_PREFIX = f"""
#
# This build constraints file was automatically generated on {now}
# for the "{DEFAULT_BRANCH}" branch of Airflow.
#
# Build constraints pin the versions of PEP 517 build-time dependencies
# (setuptools, hatchling, maturin, etc.) used during package installation
# from source distributions (sdists).
#
# Usage with uv:
#   uv pip install apache-airflow \\
#     --constraint constraints-{PYTHON_VERSION}.txt \\
#     --build-constraints build-constraints-{PYTHON_VERSION}.txt
#
# Usage with pip (>= 25.3):
#   pip install apache-airflow \\
#     --constraint constraints-{PYTHON_VERSION}.txt \\
#     --build-constraint build-constraints-{PYTHON_VERSION}.txt
#
"""


@dataclass
class ConfigParams:
    airflow_constraints_mode: str
    constraints_github_repository: str
    default_constraints_branch: str
    github_actions: bool
    python: str

    @cached_property
    def constraints_dir(self) -> Path:
        constraints_dir = Path("/files") / f"constraints-{self.python}"
        constraints_dir.mkdir(parents=True, exist_ok=True)
        return constraints_dir

    @cached_property
    def latest_constraints_file(self) -> Path:
        return self.constraints_dir / f"original-{self.airflow_constraints_mode}-{self.python}.txt"

    @cached_property
    def constraints_diff_file(self) -> Path:
        return self.constraints_dir / f"diff-{self.airflow_constraints_mode}-{self.python}.md"

    @cached_property
    def current_constraints_file(self) -> Path:
        return self.constraints_dir / f"{self.airflow_constraints_mode}-{self.python}.txt"


def install_local_airflow_with_latest_resolution(config_params: ConfigParams) -> None:
    run_command(
        [
            "uv",
            "sync",
            "--resolution",
            "highest",
            "--no-dev",
            "--package",
            "apache-airflow-core",
        ],
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_ROOT_PATH,
        check=True,
    )


def freeze_distributions_to_file(
    config_params: ConfigParams,
    file: TextIO,
    distributions_to_exclude_from_constraints: list[str] | None = None,
) -> None:
    console.print(f"[bright_blue]Freezing constraints to file: {file.name}")
    if distributions_to_exclude_from_constraints:
        console.print(
            "[bright_blue]Excluding distributions from constraints:",
            distributions_to_exclude_from_constraints,
        )
    else:
        distributions_to_exclude_from_constraints = []
    result = run_command(
        # TODO(potiuk): check if we can change this to uv
        cmd=["pip", "freeze"],
        github_actions=config_params.github_actions,
        text=True,
        check=True,
        capture_output=True,
    )
    stdout = result.stdout
    if os.environ.get("VERBOSE", "") == "true":
        if os.environ.get("CI", "") == "true":
            print("::group::Installed distributions")
        console.print("[bright_blue]Installed distributions")
        console.print(stdout)
        console.print("[bright_blue]End of installed distributions")
        if os.environ.get("CI", "") == "true":
            print("::endgroup::")
    count_lines = 0
    for line in sorted(stdout.split("\n")):
        if line.startswith(
            (
                "apache_airflow",
                "apache-airflow==",
                "apache-airflow-core==",
                "apache-airflow-task-sdk=",
                "/opt/airflow",
                "#",
                "-e",
            )
        ):
            continue
        if "@" in line:
            continue
        if "file://" in line:
            continue
        if line.strip() == "":
            continue
        if line in distributions_to_exclude_from_constraints:
            continue
        count_lines += 1
        file.write(line)
        file.write("\n")
    file.flush()
    console.print(f"[green]Constraints generated to file: {file.name}. Wrote {count_lines} lines")


def download_latest_constraint_file(config_params: ConfigParams):
    constraints_url = (
        "https://api.github.com/repos/"
        f"{config_params.constraints_github_repository}/contents/"
        f"{config_params.airflow_constraints_mode}-{config_params.python}.txt?ref={config_params.default_constraints_branch}"
    )
    # download the latest constraints file
    # download using requests
    headers = {"Accept": "application/vnd.github.v3.raw"}
    if os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ.get('GITHUB_TOKEN')}"
    else:
        console.print("[bright_blue]No GITHUB_TOKEN - using non-authenticated request.")
    console.print(f"[bright_blue]Downloading constraints file from {constraints_url}")
    r = requests.get(constraints_url, timeout=60, headers=headers)
    r.raise_for_status()
    with config_params.latest_constraints_file.open("w") as constraints_file:
        constraints_file.write(r.text)
    console.print(f"[green]Downloaded constraints file from {constraints_url} to {constraints_file.name}")


def diff_constraints(config_params: ConfigParams) -> None:
    """
    Diffs constraints files and prints the diff to the console.
    """
    console.print("[bright_blue]Diffing constraints files")
    result = run_command(
        [
            "diff",
            "--ignore-matching-lines=#",
            "--color=always",
            config_params.latest_constraints_file.as_posix(),
            config_params.current_constraints_file.as_posix(),
        ],
        # always shows output directly in CI without folded group
        github_actions=False,
        check=False,
    )
    if result.returncode == 0:
        console.print("[green]No changes in constraints files. exiting")
        config_params.constraints_diff_file.unlink(missing_ok=True)
        return
    result = run_command(
        [
            "diff",
            "--ignore-matching-lines=#",
            "--color=never",
            config_params.latest_constraints_file.as_posix(),
            config_params.current_constraints_file.as_posix(),
        ],
        github_actions=config_params.github_actions,
        check=False,
        text=True,
        capture_output=True,
    )
    with config_params.constraints_diff_file.open("w") as diff_file:
        diff_file.write(
            f"Dependencies {config_params.airflow_constraints_mode} updated "
            f"for Python {config_params.python}\n\n"
        )
        diff_file.write("```diff\n")
        diff_file.write(result.stdout)
        diff_file.write("```\n")
    console.print(f"[green]Diff generated to file: {config_params.constraints_diff_file}")


def uninstall_all_packages(config_params: ConfigParams):
    console.print("[bright_blue]Uninstall All PIP packages")
    result = run_command(
        # TODO(potiuk): check if we can change this to uv
        cmd=["pip", "freeze"],
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_ROOT_PATH,
        text=True,
        check=True,
        capture_output=True,
    )
    # do not remove installer!
    all_installed_packages = [
        dep.split("==")[0]
        for dep in result.stdout.strip().split("\n")
        if not dep.startswith(
            ("apache-airflow", "apache-airflow==", "/opt/airflow", "#", "-e", "uv==", "pip==")
        )
    ]
    run_command(
        cmd=["uv", "pip", "uninstall", *all_installed_packages],
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_ROOT_PATH,
        text=True,
        check=True,
    )


def get_all_active_provider_distributions(python_version: str | None = None) -> list[str]:
    return [
        f"apache-airflow-providers-{provider.replace('.', '-')}"
        for provider in ALL_PROVIDER_DEPENDENCIES.keys()
        if ALL_PROVIDER_DEPENDENCIES[provider]["state"] == "ready"
        and (
            python_version is None
            or python_version not in ALL_PROVIDER_DEPENDENCIES[provider]["excluded-python-versions"]
        )
    ]


def generate_constraints_source_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints with provider dependencies used from current sources. This might be different
    from the constraints generated from the latest released version of the providers in PyPI. Those
    constraints are used in CI builds when we install providers built using current sources and in
    Breeze CI image builds.
    """
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(SOURCE_PROVIDERS_CONSTRAINTS_PREFIX)
        freeze_distributions_to_file(config_params, constraints_file)
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


def get_locally_build_distribution_specs() -> list[str]:
    """
    Get all locally build distribution specification.

    This is used to exclude them from the constraints file.
    return: list of distributionss (distribution==version) to exclude from the constraints file.
    """
    all_distribution_specs = []
    all_distributions_in_dist = AIRFLOW_DIST_PATH.glob("apache_airflow_providers_*.whl")
    for dist_file_path in all_distributions_in_dist:
        version = dist_file_path.name.split("-")[1]
        distribution_name = dist_file_path.name.split("-")[0].replace("_", "-")
        all_distribution_specs.append(f"{distribution_name}=={version}")
    return all_distribution_specs


def generate_constraints_pypi_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints with provider installed from PyPI. This is the default constraints file
    used in production/release builds when we install providers from PyPI and when tagged, those
    providers are used by our users to install Airflow in reproducible way.
    :return:
    """

    # In case we have some problems with installing highest resolution of a dependency of one of our
    # providers in PyPI - we can exclude the buggy version here. For example this happened with
    # sqlalchemy-spanner==1.12.0 which did not have `whl` file in PyPI and was not installable
    # and in this case we excluded it by adding ""sqlalchemy-spanner!=1.12.0" to the list below.
    # In case we add exclusion here we should always link to the issue in the target dependency
    # repository that tracks the problem with the dependency (we should create one if it does not exist).
    #
    # Example exclusion (not needed any more as sqlalchemy-spanner==1.12.0has been yanked in PyPI):
    #
    # additional_constraints_for_highest_resolution: list[str] = ["sqlalchemy-spanner!=1.12.0"]
    #
    # Current exclusions:
    #
    # * pyarrow>=22.0.0 on Python 3.14 — older pyarrow releases have no prebuilt wheels for
    #   Python 3.14 and uv falls back to building from source, which fails. pyarrow 22.0.0 is
    #   the first release shipping cp314 wheels.
    #
    additional_constraints_for_highest_resolution: list[str] = [
        "pyarrow>=22.0.0; python_version >= '3.14'",
    ]

    result = run_command(
        cmd=[
            "uv",
            "pip",
            "install",
            "--no-sources",
            "--exact",
            "--strict",
            f"apache-airflow[all]=={AIRFLOW_VERSION}",
            f"apache-airflow-core[all]=={AIRFLOW_CORE_VERSION}",
            f"apache-airflow-task-sdk=={AIRFLOW_TASK_SDK_VERSION}",
            "./airflow-ctl",
            *additional_constraints_for_highest_resolution,
            "--reinstall",  # We need to pull the provider distributions from PyPI or dist, not the local ones
            "--resolution",
            "highest",
            "--find-links",
            "file://" + str(AIRFLOW_DIST_PATH),
        ],
        github_actions=config_params.github_actions,
        check=False,
    )
    if result.returncode != 0:
        console.print(
            "[red]Failed to install airflow with PyPI providers with highest resolution.[/]\n"
            "[yellow]Please check the output above for details. One of they ways how to resolve it, in "
            "case it is caused by a specific broken dependency version, is to exclude it above in the "
            f"`additional_constraints_for_highest_resolution` list in [/] {__file__}"
        )
        sys.exit(result.returncode)
    console.print("[success]Installed airflow with PyPI providers with eager upgrade.")
    distributions_to_exclude_from_constraints = get_locally_build_distribution_specs()
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(PYPI_PROVIDERS_CONSTRAINTS_PREFIX)
        if distributions_to_exclude_from_constraints:
            console.print(
                "[yellow]Excluding some distributions because we install them locally from build .wheels"
                "- those versions are missing from PyPI, so we need to exclude them from PyPI constraints."
            )
            # the command below prints detailed list of excluded distributions
        freeze_distributions_to_file(
            config_params, constraints_file, distributions_to_exclude_from_constraints
        )
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


def generate_constraints_no_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints without any provider dependencies. This is used mostly to generate SBOM
    files - where we generate list of dependencies for Airflow without any provider installed.
    """
    uninstall_all_packages(config_params)
    console.print(
        "[bright_blue]Installing airflow with `all-core` extras only with eager upgrade in installable mode."
    )
    install_local_airflow_with_latest_resolution(config_params)
    console.print("[success]Installed airflow with [all] extras only with eager upgrade.")
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(NO_PROVIDERS_CONSTRAINTS_PREFIX)
        freeze_distributions_to_file(config_params, constraints_file)
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


# ---------------------------------------------------------------------------
# Build constraints generation
# ---------------------------------------------------------------------------


@dataclass
class _LockPackage:
    name: str
    version: str
    sdist_url: str | None
    has_universal_wheel: bool


def _normalize_package_name(name: str) -> str:
    """Normalize package name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _extract_package_name(requirement: str) -> str:
    """Extract and normalize package name from a PEP 508 requirement string."""
    match = re.match(r"^([A-Za-z0-9][-A-Za-z0-9_.]*)", requirement.strip())
    if match:
        return _normalize_package_name(match.group(1))
    return _normalize_package_name(requirement.strip())


def _collect_workspace_build_reqs(workspace_root: Path) -> dict[str, set[str]]:
    """Step 1: Collect build-system.requires from all workspace pyproject.toml files.

    Returns a dict mapping normalized package name to a set of all distinct
    PEP 508 requirement strings seen across workspace packages.  All strings
    are passed to the resolver so it can compute their intersection.
    """
    skip_dirs = {".git", ".venv", "node_modules", "__pycache__", ".tox", ".mypy_cache", ".ruff_cache"}
    build_reqs: dict[str, set[str]] = {}
    for pyproject_path in sorted(workspace_root.glob("**/pyproject.toml")):
        if any(part in skip_dirs for part in pyproject_path.parts):
            continue
        try:
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
            for req in data.get("build-system", {}).get("requires", []):
                name = _extract_package_name(req)
                build_reqs.setdefault(name, set()).add(req)
        except Exception as e:
            console.print(f"[yellow]Warning: failed to parse {pyproject_path}: {e}")
            continue
    return build_reqs


def _parse_uv_lock(uv_lock_path: Path) -> list[_LockPackage]:
    """Parse uv.lock and return package metadata needed for build dependency scanning."""
    with open(uv_lock_path, "rb") as f:
        lock_data = tomllib.load(f)

    packages: list[_LockPackage] = []
    for pkg in lock_data.get("package", []):
        name = pkg.get("name", "")
        version = pkg.get("version", "")

        sdist = pkg.get("sdist", {})
        sdist_url = sdist.get("url") if isinstance(sdist, dict) else None

        wheels = pkg.get("wheels", [])
        has_universal = any("none-any" in w.get("url", "") for w in wheels)

        packages.append(
            _LockPackage(
                name=name,
                version=version,
                sdist_url=sdist_url,
                has_universal_wheel=has_universal,
            )
        )
    return packages


def _extract_build_reqs_from_tar(sdist_url: str) -> list[str]:
    """Stream-download a .tar.gz sdist and extract build-system.requires."""
    req = urllib.request.Request(sdist_url)
    with urllib.request.urlopen(req, timeout=120) as resp:
        with tarfile.open(fileobj=resp, mode="r|gz") as tar:
            for i, member in enumerate(tar):
                if i > 10000:
                    break
                if member.name.endswith("pyproject.toml"):
                    extracted = tar.extractfile(member)
                    if extracted:
                        data = tomllib.loads(extracted.read().decode())
                        return data.get("build-system", {}).get("requires", [])
    return []


def _extract_build_reqs_from_zip(sdist_url: str) -> list[str]:
    """Download a .zip sdist and extract build-system.requires."""
    req = urllib.request.Request(sdist_url)
    with urllib.request.urlopen(req, timeout=120) as resp:
        with zipfile.ZipFile(io.BytesIO(resp.read())) as zf:
            for name in zf.namelist():
                if name.endswith("pyproject.toml"):
                    data = tomllib.loads(zf.read(name).decode())
                    return data.get("build-system", {}).get("requires", [])
    return []


def _stream_build_reqs_from_sdist(sdist_url: str) -> list[str]:
    """Download an sdist and extract build-system.requires from its pyproject.toml.

    Supports .tar.gz (streaming) and .zip (full download). Returns the raw
    requirement strings with version specifiers preserved.

    Returns an empty list if no pyproject.toml is found (legacy sdist).
    Raises on network or parse errors so the caller can log them.
    """
    if sdist_url.endswith(".zip"):
        return _extract_build_reqs_from_zip(sdist_url)
    return _extract_build_reqs_from_tar(sdist_url)


def _collect_upstream_build_reqs(
    uv_lock_path: Path,
    cache_path: Path,
    max_workers: int = 10,
) -> dict[str, set[str]]:
    """Step 2: Scan all non-universal-wheel packages from uv.lock for build requirements.

    Uses a JSON cache keyed by ``name==version`` to avoid re-downloading
    unchanged packages on subsequent runs.

    Returns a dict mapping normalized package name to a set of all distinct
    PEP 508 requirement strings.  All strings are passed to the resolver.
    """
    # Load cache
    cache: dict[str, list[str]] = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text())
        except Exception:
            cache = {}

    # Identify targets: packages without universal wheels that have an sdist
    packages = _parse_uv_lock(uv_lock_path)
    current_keys = {
        f"{pkg.name}=={pkg.version}" for pkg in packages if not pkg.has_universal_wheel and pkg.sdist_url
    }
    targets: list[tuple[_LockPackage, str]] = []
    for pkg in packages:
        if pkg.has_universal_wheel or not pkg.sdist_url:
            continue
        if f"{pkg.name}=={pkg.version}" in cache:
            continue
        targets.append((pkg, pkg.sdist_url))

    cache_dirty = False
    failed: list[str] = []
    if targets:
        console.print(
            f"[bright_blue]Scanning {len(targets)} package sdists for build dependencies "
            f"({len(cache)} cached)..."
        )
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_stream_build_reqs_from_sdist, sdist_url): pkg for pkg, sdist_url in targets
            }
            for future in as_completed(futures):
                pkg = futures[future]
                key = f"{pkg.name}=={pkg.version}"
                try:
                    reqs = future.result()
                    if reqs:
                        cache[key] = reqs
                    else:
                        # No pyproject.toml found — legacy sdist, assume setuptools
                        console.print(f"[yellow]Warning: no pyproject.toml in {key}, assuming setuptools")
                        cache[key] = ["setuptools", "wheel"]
                    cache_dirty = True
                except Exception as e:
                    # Do NOT cache error results — let next run retry
                    console.print(f"[red]Error scanning {key} ({pkg.sdist_url}): {e}")
                    failed.append(f"{key}: {e}")

        # Persist successful results before potentially raising
        if cache_dirty:
            cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True))
            console.print(f"[green]Build dependency cache updated: {len(cache)} entries")

        if failed:
            raise RuntimeError(
                f"Failed to scan build dependencies for {len(failed)} package(s):\n"
                + "\n".join(f"  - {msg}" for msg in failed)
            )

    # Aggregate build requirements only from current uv.lock entries (not stale cache)
    all_reqs: dict[str, set[str]] = {}
    for key in current_keys:
        for req in cache.get(key, []):
            name = _extract_package_name(req)
            all_reqs.setdefault(name, set()).add(req)
    return all_reqs


def _is_exact_pin(requirement: str) -> bool:
    """Check whether a requirement string is an exact version pin (``name==X.Y.Z``).

    Exact pins are already fully reproducible in build isolation — they don't
    benefit from build constraints and may conflict with other packages'
    specifiers for the same build tool.
    """
    # Match "name==version" but NOT "name==version.*" (compatible release) or
    # "name!=version" (exclusion).  Markers after ";" are ignored.
    return bool(re.match(r"^[A-Za-z0-9][-A-Za-z0-9_.]*\s*==\s*[^\*!=,]+$", requirement.split(";")[0].strip()))


def _resolve_build_requirements(
    build_reqs: dict[str, set[str]],
    output_path: Path,
    config_params: ConfigParams,
) -> None:
    """Step 4: Resolve build requirements to pinned versions via ``uv pip compile``.

    Passes range-specifier PEP 508 requirement strings so the resolver
    respects upper bounds like ``maturin>=1.9.4,<2`` and computes their
    intersection.

    Exact-pin requirements (``name==X.Y.Z``) are excluded because they are
    already reproducible in build isolation and may conflict with other
    packages' ranges for the same build tool.

    When the resolver detects conflicting ranges for a build tool (e.g.
    ``cython>=3.0,<3.1`` vs ``cython>=3.1.2``), the conflicting package
    name is removed from the input and resolution is retried.  Skipped
    packages are left unconstrained — each package's build isolation will
    resolve them independently.
    """
    # Group range-specifier lines by normalized package name so we can
    # selectively exclude conflicting names.
    lines_by_name: dict[str, set[str]] = {}
    for name, reqs in build_reqs.items():
        for req in reqs:
            if not _is_exact_pin(req):
                lines_by_name.setdefault(name, set()).add(req)

    if not lines_by_name:
        console.print("[yellow]Warning: no range-specifier build requirements to resolve")
        output_path.write_text("")
        return

    skipped: list[str] = []
    max_retries = 10
    for attempt in range(max_retries):
        all_lines = sorted({req for reqs in lines_by_name.values() for req in reqs})
        fd, tmp_reqs_path = tempfile.mkstemp(prefix="build-reqs-", suffix=".txt")
        try:
            Path(tmp_reqs_path).write_text("\n".join(all_lines) + "\n")
            os.close(fd)

            result = run_command(
                [
                    "uv",
                    "pip",
                    "compile",
                    tmp_reqs_path,
                    "--no-config",
                    "--python-version",
                    config_params.python,
                    "--resolution",
                    "highest",
                    "--upgrade",
                    "--no-python-downloads",
                    "--no-annotate",
                    "--no-header",
                    "-o",
                    output_path.as_posix(),
                ],
                github_actions=config_params.github_actions,
                cwd=AIRFLOW_ROOT_PATH,
                check=False,
                text=True,
                capture_output=True,
            )
        finally:
            Path(tmp_reqs_path).unlink(missing_ok=True)

        if result.returncode == 0:
            break

        # Try to identify the conflicting package from the error output
        # uv error format: "Because you require <pkg>>=... and <pkg>>=..."
        conflict_name = None
        for line in (result.stderr or "").splitlines():
            match = re.search(r"you require\s+([a-zA-Z0-9][-a-zA-Z0-9_.]*)", line)
            if match:
                conflict_name = _normalize_package_name(match.group(1))
                break

        if conflict_name and conflict_name in lines_by_name:
            console.print(
                f"[yellow]Warning: conflicting build requirements for '{conflict_name}' — "
                f"removing from build constraints (each package's build isolation will "
                f"resolve it independently)"
            )
            del lines_by_name[conflict_name]
            skipped.append(conflict_name)
            continue

        # Unknown error or can't parse conflict — fail
        console.print(f"[red]{result.stderr}")
        raise RuntimeError(f"uv pip compile failed (attempt {attempt + 1}): {result.stderr}")
    else:
        raise RuntimeError(f"uv pip compile failed after {max_retries} attempts. Skipped: {skipped}")

    if skipped:
        console.print(f"[yellow]Skipped {len(skipped)} conflicting build deps: {sorted(skipped)}")


def generate_build_constraints(config_params: ConfigParams) -> None:
    """Generate build constraints by collecting and resolving all build-time dependencies."""
    console.print("[bright_blue]Generating build constraints...")

    # Step 1: Workspace build deps (local pyproject.toml files)
    workspace_reqs = _collect_workspace_build_reqs(AIRFLOW_ROOT_PATH)
    console.print(
        f"[bright_blue]Workspace build deps ({len(workspace_reqs)}): {sorted(workspace_reqs.keys())}"
    )

    # Step 2: Upstream package build deps (streaming scan with cache)
    upstream_reqs = _collect_upstream_build_reqs(
        uv_lock_path=AIRFLOW_ROOT_PATH / "uv.lock",
        cache_path=config_params.constraints_dir / "build-deps-cache.json",
    )
    console.print(f"[bright_blue]Upstream build deps ({len(upstream_reqs)}): {sorted(upstream_reqs.keys())}")

    # Step 3: Merge workspace and upstream build requirements
    all_reqs: dict[str, set[str]] = {}
    for name, reqs in workspace_reqs.items():
        all_reqs.setdefault(name, set()).update(reqs)
    for name, reqs in upstream_reqs.items():
        all_reqs.setdefault(name, set()).update(reqs)
    console.print(f"[bright_blue]Total unique build deps to resolve: {len(all_reqs)}")

    # Step 4: Resolve to pinned versions
    build_constraints_file = config_params.constraints_dir / f"build-constraints-{config_params.python}.txt"
    _resolve_build_requirements(all_reqs, build_constraints_file, config_params)

    # Step 5: Prepend header
    pinned_content = build_constraints_file.read_text()
    with build_constraints_file.open("w") as f:
        f.write(BUILD_CONSTRAINTS_PREFIX)
        f.write(pinned_content)

    console.print(f"[green]Build constraints generated: {build_constraints_file}")


ALLOWED_CONSTRAINTS_MODES = ["constraints", "constraints-source-providers", "constraints-no-providers"]


@click.command()
@click.option(
    "--airflow-constraints-mode",
    type=Choice(ALLOWED_CONSTRAINTS_MODES),
    required=True,
    envvar="AIRFLOW_CONSTRAINTS_MODE",
    help="Mode of constraints to generate",
)
@click.option(
    "--constraints-github-repository",
    default="apache/airflow",
    show_default=True,
    envvar="CONSTRAINTS_GITHUB_REPOSITORY",
    help="GitHub repository to get constraints from",
)
@click.option(
    "--default-constraints-branch",
    required=True,
    envvar="DEFAULT_CONSTRAINTS_BRANCH",
    help="Branch to get constraints from",
)
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
@click.option(
    "--python",
    required=True,
    envvar="PYTHON_MAJOR_MINOR_VERSION",
    help="Python major.minor version",
)
@click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=True,
    help="Use uv instead of pip as packaging tool.",
    envvar="USE_UV",
)
def generate_constraints(
    airflow_constraints_mode: str,
    constraints_github_repository: str,
    default_constraints_branch: str,
    github_actions: bool,
    python: str,
    use_uv: bool,
) -> None:
    config_params = ConfigParams(
        airflow_constraints_mode=airflow_constraints_mode,
        constraints_github_repository=constraints_github_repository,
        default_constraints_branch=default_constraints_branch,
        github_actions=github_actions,
        python=python,
    )
    if airflow_constraints_mode == "constraints-source-providers":
        generate_constraints_source_providers(config_params)
    elif airflow_constraints_mode == "constraints":
        generate_constraints_pypi_providers(config_params)
    elif airflow_constraints_mode == "constraints-no-providers":
        generate_constraints_no_providers(config_params)
    else:
        console.print(f"[red]Unknown constraints mode: {airflow_constraints_mode}")
        sys.exit(1)
    generate_build_constraints(config_params)
    console.print("[green]Generated constraints:")
    files = config_params.constraints_dir.rglob("*.txt")
    for file in files:
        console.print(file.as_posix())
    console.print()


if __name__ == "__main__":
    generate_constraints()
