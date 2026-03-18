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

import warnings

from airflow.providers.yandex.utils.defaults import conn_type, hook_name


def provider_user_agent() -> str | None:
    """Construct User-Agent from Airflow core & provider package versions."""
    from airflow import __version__ as airflow_version
    from airflow.providers.common.compat.sdk import conf
    from airflow.providers_manager import ProvidersManager

    try:
        manager = ProvidersManager()
        provider_name = manager.hooks[conn_type].package_name  # type: ignore[union-attr]
        provider = manager.providers[provider_name]
        return " ".join(
            (
                conf.get("yandex", "sdk_user_agent_prefix", fallback=""),
                f"apache-airflow/{airflow_version}",
                f"{provider_name}/{provider.version}",
            )
        ).strip()
    except KeyError:
        warnings.warn(
            f"Hook '{hook_name}' info is not initialized in airflow.ProviderManager",
            UserWarning,
            stacklevel=2,
        )

    return None
