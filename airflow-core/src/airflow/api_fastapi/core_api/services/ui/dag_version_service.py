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

from collections import defaultdict
from collections.abc import Sequence
from typing import Any, TypedDict, cast

import structlog
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

log = structlog.get_logger(logger_name=__name__)


class DagVersionInfo(TypedDict):
    """Info about mixed Dag versions in a DagRun."""

    has_mixed_versions: bool
    latest_version_number: int | None


class DagVersionChangeInfo(TypedDict):
    """Info about a DagRun's version change."""

    run_id: str
    dag_version_number: int | None
    dag_version_id: str | None
    is_version_changed: bool
    has_mixed_versions: bool
    latest_version_number: int | None


class DagVersionService:
    """Service class for managing Dag version operations and comparisons."""

    def __init__(self, session: SessionDep):
        self.session = session

    def detect_mixed_versions(self, dag_id: str, dag_run_ids: Sequence[str]) -> dict[str, DagVersionInfo]:
        """
        Detect mixed versions within DagRuns.

        Args:
            dag_id: The Dag ID to check
            dag_run_ids: List of DagRun IDs to analyze

        Returns:
            Dictionary mapping run_id to mixed version info
        """
        # Single optimized query to get all TaskInstance version info
        query_result = self.session.execute(
            select(TaskInstance.run_id, TaskInstance.dag_version_id, DagVersion.version_number)
            .join(DagVersion, TaskInstance.dag_version_id == DagVersion.id, isouter=True)
            .where(TaskInstance.dag_id == dag_id, TaskInstance.run_id.in_(dag_run_ids))
        )

        task_instance_versions: list[tuple[str, Any, Any]] = cast(
            "list[tuple[str, Any, Any]]", query_result.fetchall()
        )

        # Single loop to calculate mixed version info for each DagRun
        dag_run_mixed_versions: dict[str, DagVersionInfo] = {}
        run_version_map: dict[str, set[Any]] = defaultdict(set)  # version_ids per run
        run_version_numbers: dict[str, list[int]] = defaultdict(list)  # version_numbers per run

        for run_id, version_id, version_number in task_instance_versions:
            if version_id:
                run_version_map[run_id].add(version_id)
                if version_number is not None:
                    run_version_numbers[run_id].append(version_number)

        # Calculate results in a single pass
        for run_id in run_version_map:
            unique_version_ids = run_version_map[run_id]
            has_mixed_versions = len(unique_version_ids) > 1

            latest_version_number = None
            if has_mixed_versions and run_version_numbers[run_id]:
                latest_version_number = max(run_version_numbers[run_id])

            dag_run_mixed_versions[run_id] = {
                "has_mixed_versions": has_mixed_versions,
                "latest_version_number": latest_version_number,
            }

        return dag_run_mixed_versions

    def detect_version_changes(
        self, dag_runs: list[DagRun], mixed_versions_info: dict[str, DagVersionInfo]
    ) -> list[DagVersionChangeInfo]:
        """
        Detect version changes between consecutive DagRuns.

        Args:
            dag_runs: List of DagRun objects in chronological order (newest first)
            mixed_versions_info: Mixed version information from detect_mixed_versions

        Returns:
            List of dictionaries with version change information
        """
        version_changes: list[DagVersionChangeInfo] = []

        for i, dag_run in enumerate(dag_runs):
            dag_version_number = None
            dag_version_id = None
            is_version_changed = False

            # Get mixed version info for this DagRun
            mixed_info: DagVersionInfo = mixed_versions_info.get(
                dag_run.run_id, {"has_mixed_versions": False, "latest_version_number": None}
            )
            has_mixed_versions = mixed_info["has_mixed_versions"]
            latest_version_number = mixed_info["latest_version_number"]

            if dag_run.created_dag_version:
                dag_version_number = dag_run.created_dag_version.version_number
                dag_version_id = str(dag_run.created_dag_version.id)

                # Check if version changed from previous run
                next_dag_run = dag_runs[i + 1] if i + 1 < len(dag_runs) else None
                if next_dag_run and next_dag_run.created_dag_version:
                    next_version = next_dag_run.created_dag_version.version_number

                    if next_version != dag_version_number:
                        is_version_changed = True

            version_changes.append(
                {
                    "run_id": dag_run.run_id,
                    "dag_version_number": dag_version_number,
                    "dag_version_id": dag_version_id,
                    "is_version_changed": is_version_changed,
                    "has_mixed_versions": has_mixed_versions,
                    "latest_version_number": latest_version_number,
                }
            )

        # Post-process: Hide Mixed Version indicators when Version Change indicators exist
        # If a DagRun has a version change, hide the mixed version indicator of the previous DagRun
        for i, version_change in enumerate(version_changes):
            if version_change["is_version_changed"]:
                # Find the previous DagRun (next in list since it's chronologically ordered newest first)
                prev_dag_run_index = i + 1
                if prev_dag_run_index < len(version_changes):
                    # Hide the mixed version indicator of the previous DagRun
                    version_changes[prev_dag_run_index]["has_mixed_versions"] = False
                    version_changes[prev_dag_run_index]["latest_version_number"] = None

        return version_changes

    def get_version_info_for_runs(self, dag_id: str, dag_runs: list[DagRun]) -> list[DagVersionChangeInfo]:
        """
        Get complete version information for a list of DagRuns.

        Args:
            dag_id: The Dag ID
            dag_runs: List of DagRun objects

        Returns:
            List of version information dictionaries
        """
        if not dag_runs:
            return []

        dag_run_ids = [dr.run_id for dr in dag_runs]

        mixed_versions_info = self.detect_mixed_versions(dag_id, dag_run_ids)
        version_changes = self.detect_version_changes(dag_runs, mixed_versions_info)

        return version_changes
