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

from typing import TypedDict

import structlog

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.models.dagrun import DagRun

log = structlog.get_logger(logger_name=__name__)


class DagVersionInfo(TypedDict):
    """Basic info about Dag version in a DagRun."""

    dag_version_number: int | None
    dag_version_id: str | None
    is_version_changed: bool
    has_mixed_versions: bool
    latest_version_number: int | None


class DagVersionService:
    """Service class for managing Dag version operations."""

    def __init__(self, session: SessionDep):
        self.session = session

    def get_version_info_for_runs(self, dag_runs: list[DagRun]) -> list[DagVersionInfo]:
        """
        Get version information for a list of DagRuns.

        Args:
            dag_runs: List of DagRun objects in chronological order (newest first)

        Returns:
            List of dictionaries with version information
        """
        version_info_list: list[DagVersionInfo] = []

        for i, dag_run in enumerate(dag_runs):
            dag_version_number = None
            dag_version_id = None
            is_version_changed = False
            has_mixed_versions = False
            latest_version_number = None

            # Get version info from created_dag_version
            if dag_run.created_dag_version:
                dag_version_number = dag_run.created_dag_version.version_number
                dag_version_id = str(dag_run.created_dag_version.id)

                # Check if version changed from previous run
                next_dag_run = dag_runs[i + 1] if i + 1 < len(dag_runs) else None
                if next_dag_run and next_dag_run.created_dag_version:
                    next_version = next_dag_run.created_dag_version.version_number
                    if next_version != dag_version_number:
                        is_version_changed = True

            # Simple mixed version detection using dag_versions property
            dag_versions = dag_run.dag_versions
            if len(dag_versions) > 1:
                has_mixed_versions = True
                latest_version_number = max(dv.version_number for dv in dag_versions)

            version_info_list.append(
                {
                    "dag_version_number": dag_version_number,
                    "dag_version_id": dag_version_id,
                    "is_version_changed": is_version_changed,
                    "has_mixed_versions": has_mixed_versions,
                    "latest_version_number": latest_version_number,
                }
            )

        return version_info_list
