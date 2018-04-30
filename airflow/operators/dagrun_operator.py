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

from airflow.models import BaseOperator, DagBag
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings, LoggingMixin, AirflowException


class DagRunOrder(LoggingMixin):
    def __init__(self, run_id=None, payload=None, dag_id=None):
        self.run_id = run_id
        self.payload = payload
        self.dag_id = dag_id
        super().__init__()

    def trigger(self):
        if self.dag_id is None:
            raise AirflowException("{} requires a dag_id to be triggered".format(self.__class__.__name__))
        with create_session() as session:
            dbag = DagBag(settings.DAGS_FOLDER)
            trigger_dag = dbag.get_dag(self.dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=self.run_id,
                state=State.RUNNING,
                conf=self.payload,
                external_trigger=True)
            self.log.info("Creating DagRun %s", dr)
            session.add(dr)
            session.commit()


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``

    NOTE: The dag id that is triggered can be updated by the python callable
          by altering the ``dag_id`` attribute of the DagRunOrder

    :param trigger_dag_id: the dag_id to trigger
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
    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable=None,
            *args, **kwargs):
        super(TriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id

    def execute(self, context):
        dro = DagRunOrder(run_id='trig__' + timezone.utcnow().isoformat())
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        if dro:
            dro.trigger()
        else:
            self.log.info("Criteria not met, moving on")
