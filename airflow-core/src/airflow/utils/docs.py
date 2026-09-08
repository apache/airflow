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

from packaging.version import InvalidVersion, Version

# START automatically generated latest published version by supported_versions prek hook
LATEST_PUBLISHED_AIRFLOW_VERSION = "3.3.1"
# END automatically generated latest published version by supported_versions prek hook

_DOCS_BASE_URL = "https://airflow.apache.org/docs/apache-airflow/"


def _is_unpublished_docs_version(version: str) -> bool:
    try:
        parsed = Version(version)
    except InvalidVersion:
        return True
    if parsed.is_prerelease or parsed.is_postrelease or parsed.local is not None or len(parsed.release) != 3:
        return True
    return parsed > Version(LATEST_PUBLISHED_AIRFLOW_VERSION)


def get_docs_url(page: str | None = None) -> str:
    """Prepare link to Airflow documentation."""
    from airflow.version import version

    docs_version = "stable" if _is_unpublished_docs_version(version) else version
    result = f"{_DOCS_BASE_URL}{docs_version}/"
    if page:
        result = result + page
    return result
