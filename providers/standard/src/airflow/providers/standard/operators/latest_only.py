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
"""Contains an operator to run downstream tasks only for the latest scheduled DagRun."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from airflow.models import DagRun
    from airflow.timetables.base import DagRunInfo

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class LatestOnlyOperator(BaseBranchOperator):
    """
    Skip tasks that are not running during the most recent schedule interval.

    If the task is run outside the latest schedule interval (i.e. run_type == DagRunType.MANUAL),
    all directly downstream tasks will be skipped.

    Note that downstream tasks are never skipped if the given DAG_Run is
    marked as externally triggered.

    Note that when used with timetables that produce zero-length or point-in-time data intervals
    (e.g., ``DeltaTriggerTimetable``), this operator assumes each run is the latest
    and does not skip downstream tasks.
    """

    ui_color = "#e9ffdb"  # nyanza

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        # If the DAG Run is externally triggered, then return without
        # skipping downstream tasks
        dag_run: DagRun = context["dag_run"]  # type: ignore[assignment]
        if dag_run.run_type == DagRunType.MANUAL:
            self.log.info("Manually triggered DAG_Run: allowing execution to proceed.")
            return list(self.get_direct_relative_ids(upstream=False))

        next_info = self._get_next_run_info(dag_run)

        if next_info is None:
            self.log.info("Last scheduled execution: allowing execution to proceed.")
            return list(self.get_direct_relative_ids(upstream=False))

        dag_run_date = dag_run.data_interval_end or dag_run.run_after
        if dag_run_date < next_info.data_interval.end:
            self.log.info("Not latest execution, skipping downstream.")
            # we return an empty list, thus the parent BaseBranchOperator
            # won't exclude any downstream tasks from skipping.
            return []

        self.log.info("Latest, allowing execution to proceed.")
        return list(self.get_direct_relative_ids(upstream=False))

    def _get_next_run_info(self, dag_run: DagRun) -> DagRunInfo | None:
        if AIRFLOW_V_3_0_PLUS:
            from airflow.timetables.base import DataInterval, TimeRestriction

            time_restriction = TimeRestriction(earliest=None, latest=None, catchup=True)
            current_interval = DataInterval(
                start=dag_run.data_interval_start or dag_run.run_after,
                end=dag_run.data_interval_end or dag_run.run_after,
            )

            next_info = self.dag.timetable.next_dagrun_info(
                last_automated_data_interval=current_interval,
                restriction=time_restriction,
            )

        else:
            next_info = self.dag.next_dagrun_info(self.dag.get_run_data_interval(dag_run), restricted=False)
        return next_info
