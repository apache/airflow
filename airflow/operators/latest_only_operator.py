# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.state import State
from airflow import settings


class LatestOnlyOperator(BaseOperator):
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
            logging.info("""Externally triggered DAG_Run:
                         allowing execution to proceed.""")
            return

        now = datetime.datetime.now()
        left_window = context['dag'].following_schedule(
            context['execution_date'])
        right_window = context['dag'].following_schedule(left_window)
        logging.info(
            'Checking latest only with left_window: %s right_window: %s '
            'now: %s', left_window, right_window, now)

        if not left_window < now <= right_window:
            logging.info('Not latest execution, skipping downstream.')
            session = settings.Session()

            TI = TaskInstance
            tis = session.query(TI).filter(
                TI.execution_date == context['ti'].execution_date,
                TI.task_id.in_(context['task'].downstream_task_ids)
            ).with_for_update().all()

            for ti in tis:
                logging.info('Skipping task: %s', ti.task_id)
                ti.state = State.SKIPPED
                ti.start_date = now
                ti.end_date = now
                session.merge(ti)

            # this is defensive against dag runs that are not complete
            for task in context['task'].downstream_list:
                if task.task_id in tis:
                    continue

                logging.warning("Task {} was not part of a dag run. "
                                "This should not happen."
                                .format(task))
                now = datetime.datetime.now()
                ti = TaskInstance(task, execution_date=context['ti'].execution_date)
                ti.state = State.SKIPPED
                ti.start_date = now
                ti.end_date = now
                session.merge(ti)

            session.commit()
            session.close()
            logging.info('Done.')
        else:
            logging.info('Latest, allowing execution to proceed.')
