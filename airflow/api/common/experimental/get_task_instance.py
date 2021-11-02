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
"""Task Instance APIs."""
from datetime import datetime

from airflow.api.common.experimental import check_and_get_dag, check_and_get_dagrun
from airflow.exceptions import TaskInstanceNotFound
from airflow.models import TaskInstance


def get_task_instance(dag_id: str, task_id: str, execution_date: datetime) -> TaskInstance:
    """Return the task instance identified by the given dag_id, task_id and execution_date."""
    dag = check_and_get_dag(dag_id, task_id)

    dagrun = check_and_get_dagrun(dag=dag, execution_date=execution_date)
    # Get task instance object and check that it exists
    task_instance = dagrun.get_task_instance(task_id)
    if not task_instance:
        error_message = f'Task {task_id} instance for date {execution_date} not found'
        raise TaskInstanceNotFound(error_message)

    return task_instance
