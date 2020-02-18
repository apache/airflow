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
from typing import Dict, Iterable, Union

import pendulum

from airflow.operators.branch_operator import BaseBranchOperator


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

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        # If the DAG Run is externally triggered, then return without
        # skipping downstream tasks
        if context['dag_run'] and context['dag_run'].external_trigger:
            self.log.info(
                "Externally triggered DAG_Run: allowing execution to proceed.")
            return context['task'].get_direct_relative_ids(upstream=False)

        now = pendulum.utcnow()
        left_window = context['dag'].following_schedule(
            context['execution_date'])
        right_window = context['dag'].following_schedule(left_window)
        self.log.info(
            'Checking latest only with left_window: %s right_window: %s now: %s',
            left_window, right_window, now
        )

        if not left_window < now <= right_window:
            self.log.info('Not latest execution, skipping downstream.')
            # we return an empty list, thus the parent BaseBranchOperator
            # won't exclude any downstream tasks from skipping.
            return []
        else:
            self.log.info('Latest, allowing execution to proceed.')
            return context['task'].get_direct_relative_ids(upstream=False)
