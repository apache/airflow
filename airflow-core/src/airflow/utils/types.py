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

import enum
from typing import TYPE_CHECKING

import airflow.sdk.definitions._internal.types

if TYPE_CHECKING:
    from typing import TypeAlias

ArgNotSet: TypeAlias = airflow.sdk.definitions._internal.types.ArgNotSet

NOTSET = airflow.sdk.definitions._internal.types.NOTSET


class DagRunType(str, enum.Enum):
    """Class with DagRun types."""

    BACKFILL_JOB = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    ASSET_TRIGGERED = "asset_triggered"

    def __str__(self) -> str:
        return self.value

    def generate_run_id(self, *, suffix: str) -> str:
        """
        Generate a string for DagRun based on suffix string.

        :param suffix: Generate run_id from suffix.
        """
        return f"{self}__{suffix}"

    @staticmethod
    def from_run_id(run_id: str) -> DagRunType:
        """Resolve DagRun type from run_id."""
        for run_type in DagRunType:
            if run_id and run_id.startswith(f"{run_type.value}__"):
                return run_type
        return DagRunType.MANUAL


class DagRunTriggeredByType(enum.Enum):
    """Class with TriggeredBy types for DagRun."""

    CLI = "cli"  # for the trigger subcommand of the CLI: airflow dags trigger
    OPERATOR = "operator"  # for the TriggerDagRunOperator
    REST_API = "rest_api"  # for triggering the DAG via RESTful API
    UI = "ui"  # for clicking the `Trigger DAG` button
    TEST = "test"  # for dag.test()
    TIMETABLE = "timetable"  # for timetable based triggering
    ASSET = "asset"  # for asset_triggered run type
    BACKFILL = "backfill"
