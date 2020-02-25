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
"""Task APIs.."""
import re
import typing

from airflow.exceptions import DagNotFound, TaskNotFound
from airflow.models import TaskInstance
from airflow.models.dag import DagModel


def get_task(dag_id: str, task_id: str) -> TaskInstance:
    """Return the task object identified by the given dag_id and task_id."""
    dag_model = DagModel.get_dagmodel(dag_id)

    if not dag_model:
        raise DagNotFound('Dag {dag_id} not found'.format(dag_id=dag_id))

    task = dag_model.get_dag().get_task(task_id=task_id)

    if not task:
        raise TaskNotFound('Task {task_id} for dag {dag_id} not found'.format(task_id=task_id, dag_id=dag_id))

    return task


def get_downstream_tasks(dag_id: str, task_id: str, regex: str = '') -> typing.List[str]:
    """
    Returns a list of downstream tasks of the task object identified by the given dag_id and task_id.
    Optionally filter the task list with a `regex` string.
    """

    task = get_task(dag_id=dag_id, task_id=task_id)

    if regex:
        pattern = re.compile(regex)
        return list(filter(
            pattern.match,
            task.downstream_task_ids
        ))
    else:
        return list(task.downstream_task_ids)


def get_upstream_tasks(dag_id: str, task_id: str, regex: str = '') -> typing.List[str]:
    """
    Returns a list of upstream tasks of the task object identified by the given dag_id and task_id.
    Optionally filter the task list with a `regex` string.
    """

    task = get_task(dag_id=dag_id, task_id=task_id)

    if regex:
        pattern = re.compile(regex)
        return list(filter(
            pattern.match,
            task.upstream_task_ids
        ))
    else:
        return list(task.upstream_task_ids)
