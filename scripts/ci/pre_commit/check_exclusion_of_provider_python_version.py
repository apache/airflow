#!/usr/bin/env python
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
import re
import sys
from pathlib import Path
from typing import cast

import yaml
from packaging.specifiers import SpecifierSet
from packaging.version import Version

try:
    import tomllib
except ImportError:
    import tomli as tomllib

# The purpose of this script, is to check whether the exclusion of certain providers for
# specific python versions is constant across all 3 files in which it needs to be set.
# This is the providers pyproject.toml and provider.yaml and the PROVIDERS_COMPATIBILITY_TESTS_MATRIX
# in dev/breeze/src/airflow_breeze/global_constants.py

MIN_SUPPORTED_VERSION = "3.10"
SUPPORTED_PYTHON_VERSIONS = [f"3.{i}" for i in range(10, 13)]


def find_provider_root(path: Path) -> Path | None:
    current = path.parent
    while current != current.parent:
        if (current / "pyproject.toml").exists() and (current / "provider.yaml").exists():
            return current
        current = current.parent
    return None


def get_simple_provider_name(provider_root: Path) -> str:
    parts = provider_root.parts
    try:
        idx = parts.index("providers")
    except ValueError:
        return provider_root.name
    relevant_parts = parts[idx + 1 :]
    return ".".join(relevant_parts)


def filter_pyproject_exclusions(exclusions: set[str]) -> set[str]:
    filtered = set()
    for v in exclusions:
        ver = Version(v)
        if ver >= Version(
            MIN_SUPPORTED_VERSION
        ):  # If the excluded version is less than the current minimum supported one
            filtered.add(v)
    return filtered


def parse_pyproject_exclusions_filtered(pyproject_path: Path) -> set[str]:
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        requires_python = data.get("project", {}).get("requires-python", "")
        exclusions = set(re.findall(r"!=([0-9.]+)", requires_python))
        min_version_match = re.search(r">=\s*([0-9]+(?:\.[0-9]+)*)", requires_python)
        if min_version_match:
            min_version = min_version_match.group(1)
            all_py_versions = SUPPORTED_PYTHON_VERSIONS
            for v in all_py_versions:
                if Version(v) < Version(min_version):
                    exclusions.add(v)
        return filter_pyproject_exclusions(exclusions)
    except Exception as e:
        print(f"Failed to parse {pyproject_path}: {e}")
        return set()


def check_classifiers_vs_pyproject_exclusions(
    pyproject_path: Path, pyproject_exclusions: set[str]
) -> list[str]:
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        classifiers = data.get("project", {}).get("classifiers", [])
        classifier_versions = {
            entry.strip().split("::")[2].strip()
            for entry in classifiers
            if entry.strip().startswith("Programming Language :: Python ::")
            and entry.strip().count("::") == 2
        }
        return sorted(pyproject_exclusions & classifier_versions)
    except Exception as e:
        print(f"Failed to check classifiers in {pyproject_path}: {e}")
        return []


def parse_pyproject_exclusions(pyproject_path: Path) -> set[str]:
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        requires_python = data.get("project", {}).get("requires-python", "")
        spec = SpecifierSet(requires_python)

        all_py_versions = SUPPORTED_PYTHON_VERSIONS
        excluded = {v for v in all_py_versions if Version(v) not in spec}
        return excluded
    except Exception as e:
        print(f"Failed to parse {pyproject_path}: {e}")
        return set()


def parse_provider_yaml_exclusions(provider_yaml_path: Path) -> set[str]:
    try:
        with open(provider_yaml_path) as f:
            data = yaml.safe_load(f)
        raw_versions = data.get("excluded-python-versions", [])
        versions = set()
        for v in raw_versions:
            version_str = f"{v:.2f}" if isinstance(v, float) else str(v)
            if version_str in SUPPORTED_PYTHON_VERSIONS:
                versions.add(version_str)
            elif Version(version_str) < Version(MIN_SUPPORTED_VERSION):
                print(
                    f"Warning: {provider_yaml_path} excludes {version_str} which is below minimum supported version {MIN_SUPPORTED_VERSION}"
                )
        return versions
    except Exception as e:
        print(f"Failed to parse {provider_yaml_path}: {e}")
        return set()


def parse_provider_name(pyproject_path: Path) -> str | None:
    try:
        with pyproject_path.open("rb") as f:
            data = tomllib.load(f)
        return data.get("project", {}).get("name")
    except Exception as e:
        print(f"Failed to parse provider name in {pyproject_path}: {e}")
        return None


def parse_global_exclusions(global_constants_path: Path) -> dict[str, set[str]]:
    try:
        with open(global_constants_path) as f:
            tree = ast.parse(f.read())
    except Exception as e:
        print(f"Failed to parse {global_constants_path}: {e}")
        return {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets = node.targets
            if node.value is None:
                continue
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            if node.value is None:
                continue
            targets = cast("list[ast.expr]", [node.target])
            value = node.value
        else:
            continue

        for target in targets:
            if getattr(target, "id", None) == "PROVIDERS_COMPATIBILITY_TESTS_MATRIX":
                try:
                    matrix = ast.literal_eval(value)
                    version_map: dict[str, set[str]] = {}
                    for entry in matrix:
                        py_ver = entry.get("python-version")
                        removed = entry.get("remove-providers", "")
                        removed_set = set(p.strip() for p in removed.split())
                        version_map.setdefault(py_ver, set()).update(removed_set)
                    return version_map
                except Exception as e:
                    print("Failed to evaluate PROVIDERS_COMPATIBILITY_TESTS_MATRIX:", e)
                    return {}
    return {}


def check_consistency(provider_root: Path, global_exclusions: dict[str, set[str]]) -> bool:
    pyproject_path = provider_root / "pyproject.toml"
    provider_yaml_path = provider_root / "provider.yaml"
    simple_name = get_simple_provider_name(provider_root)

    pyproject_exclusions = parse_pyproject_exclusions_filtered(pyproject_path)
    provider_yaml_exclusions = parse_provider_yaml_exclusions(provider_yaml_path)
    classifier_conflicts = check_classifiers_vs_pyproject_exclusions(pyproject_path, pyproject_exclusions)

    failure_messages = []

    if pyproject_exclusions != provider_yaml_exclusions:
        failure_messages.append(
            f"\nMismatch in excluded Python versions between pyproject.toml and provider.yaml:"
            f"\n  pyproject.toml exclusions: {sorted(pyproject_exclusions) if pyproject_exclusions else 'no versions excluded'}"
            f"\n  provider.yaml exclusions: {sorted(provider_yaml_exclusions) if provider_yaml_exclusions else 'no versions excluded'}"
        )

    all_exclusions = pyproject_exclusions | provider_yaml_exclusions
    for py_ver in all_exclusions:
        if simple_name not in global_exclusions.get(py_ver, set()):
            failure_messages.append(
                f"\nProvider missing from exclusion matrix for Python {py_ver} in dev/breeze/src/airflow_breeze/global_constants.py"
            )

    if classifier_conflicts:
        failure_messages.append(
            "\nThe 'classifiers' key in pyproject.toml contains Python versions that are excluded in 'requires-python'"
        )

    if failure_messages:
        print(f"\nInconsistencies found in provider '{simple_name}':")
        for msg in failure_messages:
            print(msg)
        return False

    return True


def main():
    changed_files = [Path(f) for f in sys.argv[1:]]
    provider_roots = set()
    for f in changed_files:
        root = find_provider_root(f)
        if root:
            provider_roots.add(root)

    if not provider_roots:
        print("No relevant changes detected, skipping check.")
        sys.exit(0)

    global_constants_path = Path("dev/breeze/src/airflow_breeze/global_constants.py")
    if not global_constants_path.exists():
        print(f"Global constants file not found at {global_constants_path}")
        sys.exit(1)

    global_exclusions = parse_global_exclusions(global_constants_path)
    if not global_exclusions:
        print("Failed to parse global exclusions or global exclusions are empty.")
        sys.exit(1)

    failed = False
    for provider_root in provider_roots:
        if not check_consistency(provider_root, global_exclusions):
            failed = True

    if failed:
        sys.exit(1)
    else:
        print(
            "Python version exclusions are consistent across pyproject.toml, provider.yaml and global constants."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
