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

from datetime import datetime
import logging
import os

from airflow.models import BaseOperator, DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings
from airflow import configuration as conf


class DagRunOrder(object):
    def __init__(self, run_id=None, payload=None, execution_date=None):
        self.run_id = run_id
        self.payload = payload
        self.execution_date = execution_date


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` if a criteria is met

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
    :param execution_date: the date to run the dag for in 'YYYY-MM-DD' format.
        Defaults to ``datetime.now()``
    :type execution_date: str
    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable,
            execution_date = None,
            *args, **kwargs):
        super(TriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id
        if execution_date:
            print("Setting execution date to: " + execution_date)
            self.execution_date = datetime.strptime(execution_date, '%Y-%m-%d')
        else:
            self.execution_date = None

    def execute(self, context):
        date = self.execution_date if self.execution_date else datetime.now()
        dro = DagRunOrder(run_id='trig__' + date.isoformat(), execution_date=date)
        dro = self.python_callable(context, dro)
        if dro:
            session = settings.Session()
            dbag = DagBag(os.path.expanduser(conf.get('core', 'DAGS_FOLDER')))
            trigger_dag = dbag.get_dag(self.trigger_dag_id)
            dr = trigger_dag.create_dagrun(
                run_id=dro.run_id,
                state=State.RUNNING,
                conf=dro.payload,
                execution_date=date,
                external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")
