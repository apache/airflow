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

import logging
from airflow import settings
from airflow.utils.state import State
from airflow.utils.decorators import apply_defaults
from airflow.models import DagRun
from airflow.operators.sensors import BaseSensorOperator


class DagRunSensor(BaseSensorOperator):
    """
    Waits for a DAG run to complete.
    :param external_dag_id: The dag_id that you want to wait for
    :type external_dag_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: time difference with the previous execution to look
    at, the default is the same execution_date as the current task.  For
    yesterday, use [positive!] datetime.timedelta(days=1). Either
    execution_delta or execution_date_fn can be passed to DagRunSensor, but not
    both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
    and returns the desired execution dates to query. Either execution_delta or
    execution_date_fn can be passed to DagRunSensor, but not both.
    :type execution_date_fn: callable
    """
    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            allowed_states=None,
            execution_delta=None,
            execution_date_fn=None,
            *args, **kwargs):
        super(DagRunSensor, self).__init__(*args, **kwargs)

        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_date` or `execution_date_fn` may'
                'be provided to DagRunSensor; not both.')

        self.allowed_states = allowed_states or [State.SUCCESS]
        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id

    def poke(self, context):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context['execution_date'])
        else:
            dttm = context['execution_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ','.join([datetime.isoformat() for datetime in
                                           dttm_filter])

        logging.info('Poking for {}.{} ... '.format(self.external_dag_id,
                                                    serialized_dttm_filter))

        session = settings.Session()

        count = session.query(DagRun).filter(
            DagRun.dag_id == self.external_dag_id,
            DagRun.state.in_(self.allowed_states),
            DagRun.execution_date.in_(dttm_filter),
        ).count()

        session.commit()
        session.close()

        return count == len(dttm_filter)
