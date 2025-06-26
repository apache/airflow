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

import fnmatch
import os
from pathlib import Path

PROVIDERS_DIR = Path(__file__).parents[3].resolve() / "airflow" / "providers"


def get_removed_provider_ids() -> list[str]:
    """
    Yields the ids of suspended providers.
    """
    import yaml

    removed_provider_ids = []
    for provider_path in PROVIDERS_DIR.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml["state"] == "removed":
            removed_provider_ids.append(
                provider_yaml["package-name"][len("apache-airflow-providers-") :].replace("-", ".")
            )
    return removed_provider_ids


def find_packages_to_build(available_packages: list[str], package_filters: list[str] | None):
    """
    Filters the package list against a set of filters.

    A package is returned if it matches at least one filter. The function keeps the order of the packages.
    """
    if not package_filters:
        current_dir = Path(os.curdir).resolve()
        while not current_dir == current_dir.root:
            pyproject_toml_path = current_dir / "pyproject.toml"
            if pyproject_toml_path.exists():
                folder_name = pyproject_toml_path.parent.name
                if folder_name == "providers-summary-docs":
                    package_name = "apache-airflow-providers"
                elif folder_name == "airflow-core":
                    # Historically airflow-core documentation is built as apache-airflow
                    package_name = "apache-airflow"
                elif folder_name == "chart":
                    package_name = "helm-chart"
                else:
                    try:
                        import tomllib
                    except ImportError:
                        import tomli as tomllib
                    read_toml = tomllib.loads(pyproject_toml_path.read_text())
                    package_name = read_toml["project"]["name"]
                    if package_name == "apache-airflow":
                        # meta package
                        return available_packages
                return [package_name]
            current_dir = current_dir.parent
        return available_packages

    suspended_packages = [
        f"apache-airflow-providers-{provider.replace('.', '-')}" for provider in get_removed_provider_ids()
    ]
    all_packages_with_suspended = available_packages + suspended_packages
    invalid_filters = [
        f for f in package_filters if not any(fnmatch.fnmatch(p, f) for p in all_packages_with_suspended)
    ]
    if invalid_filters:
        raise SystemExit(
            f"Some filters did not find any package: {invalid_filters}, Please check if they are correct."
        )
    return [p for p in all_packages_with_suspended if any(fnmatch.fnmatch(p, f) for f in package_filters)]
