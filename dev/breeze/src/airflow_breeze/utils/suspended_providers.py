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

import sys

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_PROVIDERS_ROOT, AIRFLOW_SOURCES_ROOT


def get_suspended_providers_folders() -> list[str]:
    """
    Returns a list of suspended providers folders that should be
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    import yaml

    suspended_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml.get("suspended"):
            suspended_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("airflow/providers/", "")
            )
    return suspended_providers


def get_removed_provider_ids() -> list[str]:
    """
    Yields the ids of suspended providers.
    """
    import yaml

    removed_provider_ids = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.rglob("provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        package_name = provider_yaml.get("package-name")
        if provider_yaml.get("removed", False):
            if not provider_yaml.get("suspended"):
                get_console().print(
                    f"[error]The provider {package_name} is marked for removal in provider.yaml, but "
                    f"not suspended. Please suspend the provider first before removing it.\n"
                )
                sys.exit(1)
            removed_provider_ids.append(package_name[len("apache-airflow-providers-") :].replace("-", "."))
    return removed_provider_ids
