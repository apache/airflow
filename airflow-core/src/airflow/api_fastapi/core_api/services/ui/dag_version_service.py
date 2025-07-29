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

from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun

log = structlog.get_logger(logger_name=__name__)


class DagVersionInfo(TypedDict):
    """Basic info about Dag version in a DagRun."""

    dag_version_number: int | None
    dag_version_id: str | None
    is_version_changed: bool
    version_changes: list[int]


class DagVersionService:
    """Service class for managing Dag version operations."""

    def _get_latest_version(self, dag_run: DagRun) -> DagVersion | None:
        """Get the latest DagVersion used in this DagRun."""
        if dag_run.dag_versions:
            return max(dag_run.dag_versions, key=lambda dv: dv.version_number)
        return dag_run.created_dag_version

    def get_version_info_for_runs(self, dag_runs: list[DagRun]) -> list[DagVersionInfo]:
        """
        Get version information for a list of DagRuns.

        Args:
            dag_runs: List of DagRun objects in chronological order (newest first)

        Returns:
            List of dictionaries with version information
        """
        if not dag_runs:
            return []

        version_info_list: list[DagVersionInfo] = []

        # Process runs from newest to oldest, comparing with previous run
        for i, dag_run in enumerate(dag_runs):
            latest_version = self._get_latest_version(dag_run)
            current_versions = {dv.version_number for dv in dag_run.dag_versions}

            # Compare with previous run (next in chronological order)
            previous_versions = set()
            if i + 1 < len(dag_runs):
                previous_run = dag_runs[i + 1]
                previous_versions = {dv.version_number for dv in previous_run.dag_versions}

            # Calculate version changes using set difference
            version_changes = current_versions - previous_versions
            is_version_changed = bool(version_changes)

            version_info_list.append(
                {
                    "dag_version_number": latest_version.version_number if latest_version else None,
                    "dag_version_id": str(latest_version.id) if latest_version else None,
                    "is_version_changed": is_version_changed,
                    "version_changes": sorted(list(version_changes)),
                }
            )

        return version_info_list
