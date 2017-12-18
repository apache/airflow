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

from airflow.exceptions import AirflowException
from airflow.models import DagBag


def get_task(dag_id, task_id):
    """Return the task object identified by the given dag_id and task_id."""
    dagbag = DagBag()

    # Check DAG exists.
    if dag_id not in dagbag.dags:
        error_message = "Dag id {} not found".format(dag_id)
        raise AirflowException(error_message)

    # Get DAG object and check Task Exists
    dag = dagbag.get_dag(dag_id)
    if not dag.has_task(task_id):
        error_message = 'Task {} not found in dag {}'.format(task_id, dag_id)
        raise AirflowException(error_message)

    # Return the task.
    return dag.get_task(task_id)
