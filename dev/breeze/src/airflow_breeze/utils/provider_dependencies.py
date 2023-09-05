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

import json

import yaml

from airflow_breeze.utils.github import get_tag_date
from airflow_breeze.utils.path_utils import AIRFLOW_PROVIDERS_ROOT, PROVIDER_DEPENDENCIES_JSON_FILE_PATH

DEPENDENCIES = json.loads(PROVIDER_DEPENDENCIES_JSON_FILE_PATH.read_text())


def get_related_providers(
    provider_to_check: str,
    upstream_dependencies: bool,
    downstream_dependencies: bool,
) -> set[str]:
    """
    Gets cross dependencies of a provider.

    :param provider_to_check: id of the provider to check
    :param upstream_dependencies: whether to include providers that depend on it
    :param downstream_dependencies: whether to include providers it depends on
    :return: set of dependent provider ids
    """
    if not upstream_dependencies and not downstream_dependencies:
        raise ValueError("At least one of upstream_dependencies or downstream_dependencies must be True")
    related_providers = set()
    if upstream_dependencies:
        # Providers that use this provider
        for provider, provider_info in DEPENDENCIES.items():
            if provider_to_check in provider_info["cross-providers-deps"]:
                related_providers.add(provider)
    # and providers we use directly
    if downstream_dependencies:
        for dep_name in DEPENDENCIES[provider_to_check]["cross-providers-deps"]:
            related_providers.add(dep_name)
    return related_providers


def generate_providers_metadata_for_package(
    provider_id: str, constraints: dict[str, dict[str, str]]
) -> dict[str, dict[str, str]]:
    provider_yaml_dict = yaml.safe_load(
        (AIRFLOW_PROVIDERS_ROOT.joinpath(*provider_id.split(".")) / "provider.yaml").read_text()
    )
    provider_metadata: dict[str, dict[str, str]] = {}
    last_airflow_version = "2.0.0"
    package_name = "apache-airflow-providers-" + provider_id.replace(".", "-")
    for provider_version in reversed(provider_yaml_dict["versions"]):
        for airflow_version in constraints.keys():
            if constraints[airflow_version].get(package_name) == provider_version:
                last_airflow_version = airflow_version
        date_released = get_tag_date(
            tag="providers-" + provider_id.replace(".", "-") + "/" + provider_version
        )
        if date_released:
            provider_metadata[provider_version] = {
                "associated_airflow_version": last_airflow_version,
                "date_released": date_released,
            }
    return provider_metadata
