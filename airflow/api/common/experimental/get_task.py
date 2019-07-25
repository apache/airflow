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
"""Task APIs.."""
from airflow.api.common.experimental import check_and_get_dag
from airflow.models import TaskInstance


def get_task(dag_id: str, task_id: str) -> TaskInstance:
    """Return the task object identified by the given dag_id and task_id."""
    dag = check_and_get_dag(dag_id, task_id)

    # Return the task.
    return dag.get_task(task_id)


def get_task_as_dict(dag_id, task_id):
    """Return the task object as a dictionary identified by the given dag_id and task_id"""

    task = get_task(dag_id, task_id)
    fields = {k: str(v)
              for k, v in vars(task).items()
              if not k.startswith('_')}
    fields.update({
        'upstream_task_ids': list(task._upstream_task_ids),
        'downstream_task_ids': list(task._downstream_task_ids)})

    return fields
