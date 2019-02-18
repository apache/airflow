"""
SingletonOperator class

"""

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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
#     specific language governing permissions and limitations
# under the License.

from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.models.skipmixin import SkipMixin
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class SingletonOperator(BaseOperator, SkipMixin):
    """
    Allows a workflow to ensure that only single `unfinished` task is being
    executed at all times while allowing Airflow to schedule multiple DAGs to
    execute other task instances that are not directly downstream of the task.
    Unfinished task states: NONE, SCHEDULED, QUEUED, RUNNING, UP_FOR_RETRY
    All directly downstream tasks will be skipped.

    :param allow_external_trigger: If this is set, execution is allowed
        regardless of dag_run states.
    :type allow_external_trigger: Boolean
    """

    ui_color = '#e9ffdb'  # nyanza

    @apply_defaults
    def __init__(self, allow_external_trigger=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_instance = None
        self.dag_run = None
        self.allow_external_trigger = allow_external_trigger

    def execute(self, context):
        if self.allow_external_trigger and context['dag_run'].external_trigger:
            self.log.info("Externally triggered dag_run: allowing execution.")
            return

        self.task_instance = context['ti']
        self.dag_run = context['dag_run']
        downstream_tasks = self.get_flat_relatives(upstream=False)
        self.log.info("downstream_task_ids: %s", self.downstream_task_ids)
        active_dag_runs = self.get_other_active_dag_runs()
        self.log.info("Total active_dag_runs: %s", len(active_dag_runs))
        result = self.skip_or_schedule(active_dag_runs, downstream_tasks)
        self.log.info("%s execution: %s", result, self.downstream_task_ids)

    def skip_or_schedule(self, active_dag_runs, downstream_tasks):
        """
        Returns if current dag should be scheduled or skipped.

        :param active_dag_runs: list of all active dag_runs except oneself
        :param downstream_tasks: list downstream tasks

        :return: str - state - uppercase
        """
        for active_dag in active_dag_runs:
            tasks, should_skip = self.unfinished_tasks(active_dag)
            if should_skip:
                self.log.info(
                    "Found active_dag: %s with running tasks: %s, skipping...",
                    active_dag, tasks)
                self.skip(self.dag_run,
                          self.task_instance.execution_date,
                          downstream_tasks)
                # Short circuit
                return State.SKIPPED.upper()
        return State.SCHEDULED.upper()

    @provide_session
    def unfinished_tasks(self, dag_run=DagRun, session=None):
        """
        Returns unfinished downstream tasks for a dag_run.

        :param dag_run: current dag_run
        :param session: database session

        :return: list of tasks and size
        """
        tasks = session.query(TaskInstance).filter(
            DagRun.dag_id == dag_run.dag_id,
            DagRun.execution_date == dag_run.execution_date,
            TaskInstance.task_id.in_(self.downstream_task_ids),
            TaskInstance.state.in_([s for s in State.unfinished()])
        ).all()
        return tasks, len(tasks) > 0

    @provide_session
    def get_other_active_dag_runs(self, session=None):
        """
        Returns all other active dag_runs except oneself.

        :param session: database session

        :return: list of active dag_runs
        """
        return session.query(DagRun).filter(
            DagRun.dag_id == self.task_instance.dag_id,
            DagRun.state == State.RUNNING,
            DagRun.execution_date != self.task_instance.execution_date
        ).order_by(DagRun.execution_date.desc()).all()
