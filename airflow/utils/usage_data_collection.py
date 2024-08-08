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
"""
This module is for management of Airflow's usage data collection.

This module is not part of the public interface and is subject to change at any time.

:meta private:
"""

from __future__ import annotations

import platform
from urllib.parse import urlencode

import httpx
from packaging.version import parse

from airflow import __version__ as airflow_version, settings
from airflow.configuration import conf
from airflow.plugins_manager import get_plugin_info


def usage_data_collection():
    if not settings.is_usage_data_collection_enabled():
        return

    # Exclude pre-releases and dev versions
    if _version_is_prerelease(airflow_version):
        return

    scarf_domain = "https://apacheairflow.gateway.scarf.sh/scheduler"

    try:
        platform_sys, arch = get_platform_info()

        params = {
            "version": airflow_version,
            "python_version": get_python_version(),
            "platform": platform_sys,
            "arch": arch,
            "database": get_database_name(),
            "db_version": get_database_version(),
            "executor": get_executor(),
        }

        query_string = urlencode(params)
        scarf_url = f"{scarf_domain}?{query_string}"

        httpx.get(scarf_url, timeout=5.0)
    except Exception:
        pass


def _version_is_prerelease(version: str) -> bool:
    return parse(version).is_prerelease


def get_platform_info() -> tuple[str, str]:
    return platform.system(), platform.machine()


def get_database_version() -> str:
    if settings.engine is None:
        return "None"

    version_info = settings.engine.dialect.server_version_info
    # Example: (1, 2, 3) -> "1.2.3"
    return ".".join(map(str, version_info)) if version_info else "None"


def get_database_name() -> str:
    if settings.engine is None:
        return "None"
    return settings.engine.dialect.name


def get_executor() -> str:
    return conf.get("core", "EXECUTOR")


def get_python_version() -> str:
    return platform.python_version()


def get_plugin_counts() -> dict[str, int]:
    plugin_info = get_plugin_info()

    return {
        "plugins": len(plugin_info),
        "flask_blueprints": sum(len(x["flask_blueprints"]) for x in plugin_info),
        "appbuilder_views": sum(len(x["appbuilder_views"]) for x in plugin_info),
        "appbuilder_menu_items": sum(len(x["appbuilder_menu_items"]) for x in plugin_info),
        "timetables": sum(len(x["timetables"]) for x in plugin_info),
    }
