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

import yaml

from airflow_breeze.utils.path_utils import AIRFLOW_PROVIDERS_ROOT, AIRFLOW_SOURCES_ROOT


def get_suspended_providers_folders() -> list[str]:
    """
    Returns a list of suspended providers folders that should be
    skipped when running tests (without any prefix - for example apache/beam, yandex, google etc.).
    """
    suspended_providers = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.glob("**/provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml.get("suspended"):
            suspended_providers.append(
                provider_path.parent.relative_to(AIRFLOW_SOURCES_ROOT)
                .as_posix()
                .replace("airflow/providers/", "")
            )
    return suspended_providers


def get_suspended_provider_ids() -> list[str]:
    """
    Yields the ids of suspended providers.
    """
    suspended_provider_ids = []
    for provider_path in AIRFLOW_PROVIDERS_ROOT.glob("**/provider.yaml"):
        provider_yaml = yaml.safe_load(provider_path.read_text())
        if provider_yaml.get("suspended"):
            suspended_provider_ids.append(
                provider_yaml["package-name"][len("apache-airflow-providers-") :].replace("-", ".")
            )
    return suspended_provider_ids
