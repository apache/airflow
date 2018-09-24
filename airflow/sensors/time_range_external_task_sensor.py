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

from datetime import datetime, timedelta

from airflow.models import TaskInstance
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class TimeRangeExternalTaskSensor(ExternalTaskSensor):
    """
    Waits for a task to complete in a different DAG within a specific date range

    :param execution_delta: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1).
    :param execution_date_tolerance_before: time difference with the previous execution to
        look at, the default is the same execution_date as the current task.
        For yesterday, use [positive!] datetime.timedelta(days=1).
    :type execution_date_tolerance_before: datetime.timedelta
    :param execution_date_tolerance_after: time difference with an execution to
        look at in the future, the default is the same execution_date as the current task.
        For tomorrow, use [positive!] datetime.timedelta(days=1).
    :type execution_date_tolerance_after: datetime.timedelta
    """
    template_fields = ['external_dag_id', 'external_task_id']
    ui_color = '#19647e'

    @apply_defaults
    def __init__(
            self,
            execution_date_tolerance_before=None,
            execution_date_tolerance_after=None,
            *args, **kwargs):
        super(TimeRangeExternalTaskSensor, self).__init__(*args, **kwargs)
        self.execution_date_tolerance_before = execution_date_tolerance_before if execution_date_tolerance_before else timedelta(seconds=0)
        self.execution_date_tolerance_after = execution_date_tolerance_after if execution_date_tolerance_after else timedelta(seconds=0)

    @provide_session
    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        else:
            dttm = context['execution_date']

        self.log.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} on '
            '{dttm} ... '.format(**locals()))
        TI = TaskInstance

        execution_date_lower_bound = dttm - self.execution_date_tolerance_before
        execution_date_upper_bound = dttm + self.execution_date_tolerance_after
        self.log.info('Searching for matching task(s) with execution_date between [{}, {}]...'.format(execution_date_lower_bound, execution_date_upper_bound))

        session = settings.Session()
        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date >= execution_date_lower_bound,
            TI.execution_date <= execution_date_upper_bound
        ).count()
        session.commit()
        session.close()
        return count
