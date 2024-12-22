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

import os
import platform
from urllib.parse import urlencode

import httpx
from packaging.version import parse

from airflow import __version__ as airflow_version, settings
from airflow.configuration import conf


def usage_data_collection():
    if not settings.is_usage_data_collection_enabled():
        return

    # Exclude pre-releases and dev versions
    if _version_is_prerelease(airflow_version):
        return

    # Exclude CI environments
    if _is_ci_environ():
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


def _is_ci_environ() -> bool:
    """Return True if running in any known CI environment."""
    if os.getenv("CI") == "true":
        # Generic CI variable set by many CI systems (GH Actions, Travis, GitLab, CircleCI, Jenkins, Heroku)
        return True

    # Other CI variables set by specific CI systems
    ci_env_vars = {
        "CIRCLECI",  # CircleCI
        "CODEBUILD_BUILD_ID",  # AWS CodeBuild
        "GITHUB_ACTIONS",  # GitHub Actions
        "GITLAB_CI",  # GitLab CI
        "JENKINS_URL",  # Jenkins
        "TF_BUILD",  # Azure Pipelines
        "TRAVIS",  # Travis CI
    }

    return any(var in os.environ for var in ci_env_vars)


def get_platform_info() -> tuple[str, str]:
    return platform.system(), platform.machine()


def get_database_version() -> str:
    if settings.engine is None:
        return "None"

    version_info = settings.engine.dialect.server_version_info
    # Example: (1, 2, 3) -> "1.2" (cut only major+minor w/o patch)
    return ".".join(map(str, version_info[0:2])) if version_info else "None"


def get_database_name() -> str:
    if settings.engine is None:
        return "None"
    return settings.engine.dialect.name


def get_executor() -> str:
    return conf.get("core", "EXECUTOR")


def get_python_version() -> str:
    # Cut only major+minor from the python version string (e.g. 3.10.12 --> 3.10)
    return ".".join(platform.python_version().split(".")[0:2])
