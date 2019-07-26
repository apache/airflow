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
"""Task Instance APIs Helper"""

from airflow.exceptions import (DagNotFound, TaskNotFound,
                                DagRunNotFound, TaskInstanceNotFound)
from airflow.models import DagBag
from airflow.models import TaskInstance
from airflow.utils import timezone


def get_task_instance(dag_id, task_id, execution_date):
    """
    Return the task object identified by the given dag_id and task_id.

    :param dag_id: String identifier of a DAG
    :param task_id: String identifier of a task
    :param execution_date: date to identify dag run
        should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    """

    dagbag = DagBag()

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise DagNotFound(error_message)

    # Get DAG object and check Task Exists
    dag = dagbag.get_dag(dag_id)
    if not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        raise TaskNotFound(error_message)

    # Get DagRun object and check that it exists
    dagrun = dag.get_dagrun(execution_date=execution_date)
    if not dagrun:
        error_message = ('Dag Run for date {} not found in dag {}'
                         .format(execution_date, dag_id))
        raise DagRunNotFound(error_message)

    # Get task instance object and check that it exists
    task_instance = dagrun.get_task_instance(task_id)
    if not task_instance:
        error_message = ('Task {} instance for date {} not found'
                         .format(task_id, execution_date))
        raise TaskInstanceNotFound(error_message)

    return task_instance


def get_all_task_instances(dag_id=None, state=None, state_ne=None, execution_date_before=None,
                           execution_date_after=None, task_id=None):
    """
    Returns a list of Dag Runs for a specific DAG ID.

    :param dag_id: String identifier of a DAG
    :param task_id: String identifier of a task
    :param state: queued|running|success...
    :param state_ne: queued|running|success...
    :param execution_date_before: a query string parameter to find all runs before provided date,
        should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    :param execution_date_after: a query string parameter to find all runs after provided date,
        should be in format "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15".
    :return: List of task instances
    """

    task_instances = list()
    state = state.lower() if state else None
    state_ne = state_ne.lower() if state_ne else None
    execution_date_before = timezone.parse(execution_date_before) if execution_date_before else None
    execution_date_after = timezone.parse(execution_date_after) if execution_date_after else None
    for instance in TaskInstance.find(dag_id=dag_id, state=state, state_ne=state_ne,
                                      execution_date_before=execution_date_before,
                                      execution_date_after=execution_date_after, task_id=task_id):
        fields = {k: str(v)
                  for k, v in vars(instance).items()
                  if not k.startswith('_')}
        fields.update({'try_number': instance.try_number})
        task_instances.append(fields)

    return task_instances
