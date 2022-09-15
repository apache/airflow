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
"""
This module contains an operator to run downstream tasks only for the
latest scheduled DagRun
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

import pendulum

from airflow.operators.branch import BaseBranchOperator
from airflow.utils.context import Context

if TYPE_CHECKING:
    from airflow.models import DAG, DagRun


class LatestOnlyOperator(BaseBranchOperator):
    """
    Allows a workflow to skip tasks that are not running during the most
    recent schedule interval.

    If the task is run outside of the latest schedule interval (i.e. external_trigger),
    all directly downstream tasks will be skipped.

    Note that downstream tasks are never skipped if the given DAG_Run is
    marked as externally triggered.
    """

    ui_color = '#e9ffdb'  # nyanza

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        # If the DAG Run is externally triggered, then return without
        # skipping downstream tasks
        dag_run: DagRun = context["dag_run"]
        if dag_run.external_trigger:
            self.log.info("Externally triggered DAG_Run: allowing execution to proceed.")
            return list(context['task'].get_direct_relative_ids(upstream=False))

        dag: DAG = context["dag"]
        next_info = dag.next_dagrun_info(dag.get_run_data_interval(dag_run), restricted=False)
        now = pendulum.now('UTC')

        if next_info is None:
            self.log.info("Last scheduled execution: allowing execution to proceed.")
            return list(context['task'].get_direct_relative_ids(upstream=False))

        left_window, right_window = next_info.data_interval
        self.log.info(
            'Checking latest only with left_window: %s right_window: %s now: %s',
            left_window,
            right_window,
            now,
        )

        if not left_window < now <= right_window:
            self.log.info('Not latest execution, skipping downstream.')
            # we return an empty list, thus the parent BaseBranchOperator
            # won't exclude any downstream tasks from skipping.
            return []
        else:
            self.log.info('Latest, allowing execution to proceed.')
            return list(context['task'].get_direct_relative_ids(upstream=False))
