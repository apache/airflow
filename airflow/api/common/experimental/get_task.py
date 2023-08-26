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
"""Task APIs."""
from __future__ import annotations

from typing import TYPE_CHECKING

from deprecated import deprecated

from airflow.api.common.experimental import check_and_get_dag

if TYPE_CHECKING:
    from airflow.models import TaskInstance


@deprecated(reason="Use DAG().get_task", version="2.2.4")
def get_task(dag_id: str, task_id: str) -> TaskInstance:
    """Return the task object identified by the given dag_id and task_id."""
    dag = check_and_get_dag(dag_id, task_id)

    # Return the task.
    return dag.get_task(task_id)
