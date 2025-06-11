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

from packaging.version import Version

from airflow import __version__

AIRFLOW_VERSION = Version(__version__)


def get_expected_event_file_path(dag_id: str) -> str:
    """
    Retrieve the file path to the OpenLineage expected events JSON file for a given DAG ID,
    taking into account the Airflow version currently in use.

    Since expected event outputs may differ between Airflow versions, this function attempts
    to locate the most specific expected event file available for the running Airflow version,
    following this precedence order:

    1. A file named `{dag_id}__af{major_version}_{minor_version}.json`
        (e.g., `example_dag__af2_10.json` for Airflow 2.10.x)
    2. A file named `{dag_id}__af{major_version}.json`
        (e.g., `example_dag__af3.json` for any Airflow 3.x version)
    3. A generic file named `{dag_id}.json` without version suffix
        (e.g., `example_dag.json` for any Airflow version)

    The function returns the path to the first existing file found in this order.
    We expect all the files to follow the naming convention described above.

    If none of the expected files exist, it raises a ValueError indicating that no matching
    expected event file could be found.

    Args:
        dag_id: The identifier of the DAG whose expected event file is to be retrieved.

    Returns:
        The file path to the appropriate expected events JSON file.

    Raises:
        ValueError: If no expected event files matching the DAG ID and Airflow version are found.
    """
    base_path = Path(__file__).parent

    paths_to_check = (
        str(base_path / f"{dag_id}__af{AIRFLOW_VERSION.major}_{AIRFLOW_VERSION.minor}.json"),
        str(base_path / f"{dag_id}__af{AIRFLOW_VERSION.major}.json"),
        str(base_path / f"{dag_id}.json"),
    )

    for path in paths_to_check:
        if os.path.exists(path):
            return path

    raise ValueError(
        f"Could not locate expected event files for dag_id {dag_id}. "
        f"Expected one of the following files: `{paths_to_check}`"
    )
