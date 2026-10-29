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

import os
from pathlib import Path

from sphinx_exts.provider_yaml_utils import load_package_data

AIRFLOW_CONTENT_ROOT_PATH = Path(__file__).parents[4].resolve()
ALL_PROVIDER_YAMLS = load_package_data()
ALL_PROVIDER_YAMLS_WITH_SUSPENDED = load_package_data(include_suspended=True)
DEVEL_COMMON_PATH = AIRFLOW_CONTENT_ROOT_PATH / "devel-common"
GENERATED_PATH = AIRFLOW_CONTENT_ROOT_PATH / "generated"
DOCS_SOURCES_PATH = DEVEL_COMMON_PATH / "src" / "docs"

AIRFLOW_SITE_DIR: str = os.environ.get("AIRFLOW_SITE_DIRECTORY") or ""
PROCESS_TIMEOUT = 15 * 60 * 2

CONSOLE_WIDTH = 250


def pretty_format_path(path: str, start: str) -> str:
    """Formats path nicely."""
    relpath = os.path.relpath(path, start)
    if relpath == path:
        return path
    return f"{start}/{relpath}"
