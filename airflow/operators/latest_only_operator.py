# -*- coding: utf-8 -*-
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

from airflow.models import BaseOperator, SkipMixin
from airflow.utils import timezone


class LatestOnlyOperator(BaseOperator, SkipMixin):
    """
    Allows a workflow to skip tasks that are not running during the most
    recent schedule interval.

    If the task is run outside of the latest schedule interval, all
    directly downstream tasks will be skipped.
    """

    ui_color = '#e9ffdb'  # nyanza

    def execute(self, context):
        # If the DAG Run is externally triggered, then return without
        # skipping downstream tasks
        if context['dag_run'] and context['dag_run'].external_trigger:
            self.log.info("Externally triggered DAG_Run: allowing execution to proceed.")
            return

        now = timezone.utcnow()
        left_window = context['dag'].following_schedule(
            context['execution_date'])
        right_window = context['dag'].following_schedule(left_window)
        self.log.info(
            'Checking latest only with left_window: %s right_window: %s now: %s',
            left_window, right_window, now
        )

        if not left_window < now <= right_window:
            self.log.info('Not latest execution, skipping downstream.')

            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            self.log.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)

            self.log.info('Done.')
        else:
            self.log.info('Latest, allowing execution to proceed.')
