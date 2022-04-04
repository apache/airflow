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
Useful tools for various Paths used inside Airflow Sources.
"""

import os
import tempfile
from pathlib import Path
from typing import Optional

from airflow_breeze.utils.console import console

AIRFLOW_CFG_FILE = "setup.cfg"


def search_upwards_for_airflow_sources_root(start_from: Path) -> Optional[Path]:
    root = Path(start_from.root)
    d = start_from
    while d != root:
        attempt = d / AIRFLOW_CFG_FILE
        if attempt.exists() and "name = apache-airflow\n" in attempt.read_text():
            return attempt.parent
        d = d.parent
    return None


def find_airflow_sources_root() -> Path:
    """
    Find the root of airflow sources. When Breeze is run from sources, it is easy, but this one also
    has to handle the case when Breeze is installed via `pipx` so it searches upwards of the current
    directory to find the right root of airflow directory.

    If not found, current directory is returned (this handles the case when Breeze is run from the local
    directory.

    :return: Path for the found sources.

    """
    default_airflow_sources_root = Path.cwd()
    # Try to find airflow sources in current working dir
    airflow_sources_root = search_upwards_for_airflow_sources_root(Path.cwd())
    if not airflow_sources_root:
        # Or if it fails, find it in parents of the directory where the ./breeze.py is.
        airflow_sources_root = search_upwards_for_airflow_sources_root(Path(__file__).resolve().parent)
    if airflow_sources_root:
        os.chdir(airflow_sources_root)
        return Path(airflow_sources_root)
    else:
        console.print(
            f"\n[bright_yellow]Could not find Airflow sources location. "
            f"Assuming {default_airflow_sources_root}"
        )
    os.chdir(default_airflow_sources_root)
    return Path(default_airflow_sources_root)


AIRFLOW_SOURCES_ROOT = find_airflow_sources_root()

BUILD_CACHE_DIR = AIRFLOW_SOURCES_ROOT / '.build'
FILES_DIR = AIRFLOW_SOURCES_ROOT / 'files'
MSSQL_DATA_VOLUME = AIRFLOW_SOURCES_ROOT / 'tmp_mssql_volume'
MYPY_CACHE_DIR = AIRFLOW_SOURCES_ROOT / '.mypy_cache'
LOGS_DIR = AIRFLOW_SOURCES_ROOT / 'logs'
DIST_DIR = AIRFLOW_SOURCES_ROOT / 'dist'
SCRIPTS_CI_DIR = AIRFLOW_SOURCES_ROOT / 'scripts' / 'ci'
DOCKER_CONTEXT_DIR = AIRFLOW_SOURCES_ROOT / 'docker-context-files'
CACHE_TMP_FILE_DIR = tempfile.TemporaryDirectory()
OUTPUT_LOG = Path(CACHE_TMP_FILE_DIR.name, 'out.log')


def create_directories() -> None:
    """
    Creates all directories that are needed for Breeze to work.
    """
    BUILD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    MSSQL_DATA_VOLUME.mkdir(parents=True, exist_ok=True)
    MYPY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_LOG.mkdir(parents=True, exist_ok=True)
