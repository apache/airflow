#
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

import platform

import httpx
from packaging.version import parse

from airflow import __version__ as airflow_version, settings
from airflow.configuration import conf


def scarf_analytics():
    if not settings.is_scarf_analytics_enabled():
        return

    # Exclude pre-releases and dev versions
    if _version_is_prerelease(airflow_version):
        return

    scarf_domain = "https://apacheairflow.gateway.scarf.sh/scheduler"

    db_version = settings.engine.dialect.server_version_info
    if db_version:
        # Example: (1, 2, 3) -> "1.2.3"
        db_version = ".".join(map(str, db_version))

    try:
        scarf_url = (
            f"{scarf_domain}?version={airflow_version}"
            f"&python_version={platform.python_version()}"
            f"&platform={platform.system()}"
            f"&arch={platform.machine()}"
            f"&database={settings.engine.dialect.name}"
            f"&db_version={db_version}"
            f"&executor={conf.get('core', 'EXECUTOR')}"
        )

        httpx.get(scarf_url, timeout=5.0)
    except Exception:
        pass


def _version_is_prerelease(version: str) -> bool:
    return parse(version).is_prerelease
