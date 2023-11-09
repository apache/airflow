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
import json
from typing import Iterable

from airflow_breeze.global_constants import REGULAR_DOC_PACKAGES
from airflow_breeze.utils.path_utils import PROVIDER_DEPENDENCIES_JSON_FILE_PATH
from airflow_breeze.utils.suspended_providers import get_removed_provider_ids

LONG_PROVIDERS_PREFIX = "apache-airflow-providers-"


def get_available_packages(
    include_non_provider_doc_packages: bool = False, include_all_providers: bool = False
) -> list[str]:
    provider_ids: list[str] = list(json.loads(PROVIDER_DEPENDENCIES_JSON_FILE_PATH.read_text()).keys())
    available_packages = []
    if include_non_provider_doc_packages:
        available_packages.extend(REGULAR_DOC_PACKAGES)
    if include_all_providers:
        available_packages.append("all-providers")
    available_packages.extend(provider_ids)
    return available_packages


def expand_all_provider_packages(short_doc_packages: tuple[str, ...]) -> tuple[str, ...]:
    if "all-providers" in short_doc_packages:
        packages = [package for package in short_doc_packages if package != "all-providers"]
        packages.extend(get_available_packages())
        short_doc_packages = tuple(set(packages))
    return short_doc_packages


def get_long_package_names(short_form_providers: Iterable[str]) -> tuple[str, ...]:
    providers: list[str] = []
    for short_form_provider in short_form_providers:
        if short_form_provider in REGULAR_DOC_PACKAGES:
            providers.append(short_form_provider)
            continue
        short_form_provider.split(".")
        parts = "-".join(short_form_provider.split("."))
        providers.append(LONG_PROVIDERS_PREFIX + parts)
    return tuple(providers)


def convert_to_long_package_names(
    package_filters: tuple[str, ...], packages_short_form: tuple[str, ...]
) -> tuple[str, ...]:
    """Filters the package list against a set of filters.

    A packet is returned if it matches at least one filter. The function keeps the order of the packages.
    """
    available_doc_packages = list(
        get_long_package_names(get_available_packages(include_non_provider_doc_packages=True))
    )
    if not package_filters and not packages_short_form:
        available_doc_packages.extend(package_filters)
        return tuple(set(available_doc_packages))

    processed_package_filters = list(package_filters)
    processed_package_filters.extend(get_long_package_names(packages_short_form))

    removed_packages: list[str] = [
        f"apache-airflow-providers-{provider.replace('.','-')}" for provider in get_removed_provider_ids()
    ]
    all_packages_including_removed: list[str] = available_doc_packages + removed_packages
    invalid_filters = [
        f
        for f in processed_package_filters
        if not any(fnmatch.fnmatch(p, f) for p in all_packages_including_removed)
    ]
    if invalid_filters:
        raise SystemExit(
            f"Some filters did not find any package: {invalid_filters}, Please check if they are correct."
        )

    return tuple(
        [
            p
            for p in all_packages_including_removed
            if any(fnmatch.fnmatch(p, f) for f in processed_package_filters)
        ]
    )
