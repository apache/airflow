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
from datetime import timedelta
from typing import TYPE_CHECKING

import pendulum

from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS
from airflow.utils.types import DagRunType

if TYPE_CHECKING:
    from pendulum.datetime import DateTime

    from airflow.models import DagRun
    from airflow.providers.common.compat.sdk import Context

if AIRFLOW_V_3_2_PLUS:

    def _get_dag_timetable(dag):
        from airflow.serialization.encoders import coerce_to_core_timetable

        return coerce_to_core_timetable(dag.timetable)
else:

    def _get_dag_timetable(dag):
        return dag.timetable


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

        dates = self._get_compare_dates(dag_run)

        if dates is None:
            self.log.info("Last scheduled execution: allowing execution to proceed.")
            return list(self.get_direct_relative_ids(upstream=False))

        now = pendulum.now("UTC")
        left_window, right_window = dates
        self.log.info(
            "Checking latest only with left_window: %s right_window: %s now: %s",
            left_window,
            right_window,
            now,
        )

        if not left_window < now <= right_window:
            self.log.info("Not latest execution, skipping downstream.")
            # we return an empty list, thus the parent BaseBranchOperator
            # won't exclude any downstream tasks from skipping.
            return []

        self.log.info("Latest, allowing execution to proceed.")
        return list(self.get_direct_relative_ids(upstream=False))

    def _get_compare_dates(self, dag_run: DagRun) -> tuple[DateTime, DateTime] | None:
        dagrun_date: DateTime
        if AIRFLOW_V_3_0_PLUS:
            dagrun_date = dag_run.logical_date or dag_run.run_after  # type: ignore[assignment]
        else:
            dagrun_date = dag_run.logical_date  # type: ignore[assignment]

        from airflow.timetables.base import DataInterval, TimeRestriction

        if dag_run.data_interval_start:
            start = pendulum.instance(dag_run.data_interval_start)
        else:
            start = dagrun_date

        if dag_run.data_interval_end:
            end = pendulum.instance(dag_run.data_interval_end)
        else:
            end = dagrun_date

        timetable = _get_dag_timetable(self.dag)
        current_interval = DataInterval(start=start, end=end)
        time_restriction = TimeRestriction(
            earliest=None, latest=current_interval.end - timedelta(microseconds=1), catchup=True
        )

        if prev_info := timetable.next_dagrun_info(
            last_automated_data_interval=current_interval,
            restriction=time_restriction,
        ):
            left = prev_info.data_interval.end
        else:
            left = current_interval.start

        time_restriction = TimeRestriction(earliest=current_interval.end, latest=None, catchup=True)
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=current_interval,
            restriction=time_restriction,
        )

        if not next_info:
            return None

        return (left, next_info.data_interval.end)
