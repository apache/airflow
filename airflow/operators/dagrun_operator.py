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

import datetime
import six
from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.api.common.experimental.trigger_dag import trigger_dag

import json


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None):
        self.run_id = run_id
        self.payload = payload


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``

    :param trigger_dag_id: the dag_id to trigger (templated)
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    :param execution_date: Execution date for the dag (templated)
    :type execution_date: str or datetime.datetime
    """
    template_fields = ('trigger_dag_id', 'execution_date')
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable=None,
            execution_date=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id

        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, six.string_types):
            self.execution_date = execution_date
        elif execution_date is None:
            self.execution_date = execution_date
        else:
            raise TypeError(
                'Expected str or datetime.datetime type '
                'for execution_date. Got {}'.format(
                    type(execution_date)))

    def execute(self, context):
        if self.execution_date is not None:
            run_id = 'trig__{}'.format(self.execution_date)
            self.execution_date = timezone.parse(self.execution_date)
        else:
            run_id = 'trig__' + timezone.utcnow().isoformat()
        dro = DagRunOrder(run_id=run_id)
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        if dro:
            trigger_dag(dag_id=self.trigger_dag_id,
                        run_id=dro.run_id,
                        conf=json.dumps(dro.payload),
                        execution_date=self.execution_date,
                        replace_microseconds=False)
        else:
            self.log.info("Criteria not met, moving on")
