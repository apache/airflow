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
# requires-python = ">=3.8"
# dependencies = [
#     "pyyaml>=6.0",
# ]
# ///

"""
This script retrieves versions from versioned doc packages built and stores them in stable.txt files.

It should be run after building docs but before saving/uploading the build artifacts.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

import yaml


def get_airflow_version(airflow_root: Path) -> str | None:
    """Get Airflow version from airflow/__init__.py."""
    # Try Airflow 3.x location first
    init_file = airflow_root / "airflow-core" / "src" / "airflow" / "__init__.py"
    if not init_file.exists():
        # Fallback to Airflow 2.x location
        init_file = airflow_root / "airflow" / "__init__.py"

    if not init_file.exists():
        return None

    content = init_file.read_text()
    match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if match:
        return match.group(1)
    return None


def get_version_from_provider_yaml(provider_yaml_path: Path) -> str | None:
    """Get version from provider.yaml file (first version in the versions list)."""
    if not provider_yaml_path.exists():
        return None

    try:
        with open(provider_yaml_path) as f:
            data = yaml.safe_load(f)
            if "versions" in data and len(data["versions"]) > 0:
                # versions is a list of version strings, get the first one
                return str(data["versions"][0])
    except Exception:
        pass
    return None


def get_version_from_pyproject_toml(pyproject_path: Path) -> str | None:
    """Get version from pyproject.toml file."""
    if not pyproject_path.exists():
        return None

    content = pyproject_path.read_text()
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if match:
        return match.group(1)
    return None


def get_helm_chart_version(chart_yaml_path: Path) -> str | None:
    """Get version from Chart.yaml file."""
    if not chart_yaml_path.exists():
        return None

    content = chart_yaml_path.read_text()
    match = re.search(r"^version:\s*(.+)$", content, re.MULTILINE)
    if match:
        return match.group(1).strip()
    return None


def get_package_version(package_name: str, airflow_root: Path) -> str | None:
    """Get version for a package based on its type and metadata location."""
    if package_name == "apache-airflow":
        return get_airflow_version(airflow_root)

    if package_name == "apache-airflow-ctl":
        # Try provider.yaml first
        provider_yaml = airflow_root / "airflow-ctl" / "src" / "airflow_ctl" / "provider.yaml"
        version = get_version_from_provider_yaml(provider_yaml)
        if version:
            return version
        # Fallback to pyproject.toml
        pyproject = airflow_root / "airflow-ctl" / "pyproject.toml"
        return get_version_from_pyproject_toml(pyproject)

    if package_name == "task-sdk":
        # Try provider.yaml first
        provider_yaml = airflow_root / "task-sdk" / "src" / "task_sdk" / "provider.yaml"
        version = get_version_from_provider_yaml(provider_yaml)
        if version:
            return version
        # Fallback to pyproject.toml
        pyproject = airflow_root / "task-sdk" / "pyproject.toml"
        return get_version_from_pyproject_toml(pyproject)

    if package_name == "helm-chart":
        chart_yaml = airflow_root / "chart" / "Chart.yaml"
        return get_helm_chart_version(chart_yaml)

    if package_name.startswith("apache-airflow-providers-"):
        # Get provider version from provider.yaml
        provider_short_name = package_name.replace("apache-airflow-providers-", "").replace("-", "/")

        # Try Airflow 3.x location first (providers/{provider}/provider.yaml)
        provider_yaml = airflow_root / "providers" / provider_short_name / "provider.yaml"
        version = get_version_from_provider_yaml(provider_yaml)
        if version:
            return version

        # Fallback to Airflow 2.x location (airflow/providers/{provider}/provider.yaml)
        provider_yaml = airflow_root / "airflow" / "providers" / provider_short_name / "provider.yaml"
        return get_version_from_provider_yaml(provider_yaml)

    print(f"Unknown package type: {package_name}")
    return None


def main() -> int:
    """Main function to process all documentation packages."""
    # Get configuration from environment or defaults
    docs_build_dir = Path(os.environ.get("DOCS_BUILD_DIR", "generated/_build/docs"))
    airflow_root = Path(os.environ.get("AIRFLOW_ROOT", os.getcwd()))

    # Change to airflow root directory
    os.chdir(airflow_root)

    print("=" * 42)
    print("Storing stable versions for built docs")
    print("=" * 42)

    # Check if docs build directory exists
    if not docs_build_dir.exists():
        print(f"Error: Docs build directory not found at {docs_build_dir}")
        # Try alternate location for Airflow 2 compatibility
        alt_docs_dir = Path("docs/_build/docs")
        if alt_docs_dir.exists():
            docs_build_dir = alt_docs_dir
            print(f"Found alternate location at {docs_build_dir}")
        else:
            print("No docs build directory found, exiting")
            return 1

    # Non-versioned packages to skip
    non_versioned_packages = {"apache-airflow-providers", "docker-stack"}

    stable_files_created = []

    # Process each package in the docs build directory
    for package_dir in sorted(docs_build_dir.iterdir()):
        if not package_dir.is_dir():
            continue

        package_name = package_dir.name

        # Skip non-versioned packages
        if package_name in non_versioned_packages:
            print(f"Skipping non-versioned package: {package_name}")
            continue

        # Check if this package has a stable directory (indicating it's versioned)
        stable_dir = package_dir / "stable"
        if not stable_dir.exists() or not stable_dir.is_dir():
            print(f"Skipping non-versioned package (no stable dir): {package_name}")
            continue

        print(f"Processing versioned package: {package_name}")

        # Get the version for this package
        version = get_package_version(package_name, airflow_root)

        if not version:
            print(f"  Warning: Could not determine version for {package_name}, skipping")
            continue

        print(f"  Version: {version}")

        # Create stable.txt file
        stable_file = package_dir / "stable.txt"
        stable_file.write_text(version + "\n")
        print(f"  Created: {stable_file}")
        stable_files_created.append((package_name, version))

        # Also create a version-specific copy of the stable docs
        version_dir = package_dir / version
        if not version_dir.exists():
            print(f"  Copying stable docs to versioned directory: {version_dir}")
            shutil.copytree(stable_dir, version_dir)
        else:
            print(f"  Version directory already exists: {version_dir}")

    print()
    print("=" * 42)
    print("Stable version files created successfully")
    print("=" * 42)
    print()

    if stable_files_created:
        print("Summary of stable.txt files:")
        for package_name, version in stable_files_created:
            print(f"  {package_name}: {version}")
    else:
        print("No stable.txt files created")

    print()
    print("Done!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
