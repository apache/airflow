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
        if provider_yaml.get("removed"):
            removed_provider_ids.append(
                provider_yaml["package-name"][len("apache-airflow-providers-") :].replace("-", ".")
            )
    return removed_provider_ids


def process_package_filters(available_packages: list[str], package_filters: list[str] | None):
    """Filters the package list against a set of filters.

    A packet is returned if it matches at least one filter. The function keeps the order of the packages.
    """
    if not package_filters:
        return available_packages

    suspended_packages = [
        f"apache-airflow-providers-{provider.replace('.','-')}" for provider in get_removed_provider_ids()
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
