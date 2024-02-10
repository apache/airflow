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
"""Experimental APIs."""
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import DagNotFound, DagRunNotFound, TaskNotFound
from airflow.models import DagBag, DagModel

if TYPE_CHECKING:
    from datetime import datetime

    from airflow.models import DagRun


def check_and_get_dag(dag_id: str, task_id: str | None = None) -> DagModel:
    """Check DAG existence and in case it is specified that Task exists."""
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise DagNotFound(f"Dag id {dag_id} not found in DagModel")

    dagbag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
    dag = dagbag.get_dag(dag_id)
    if not dag:
        error_message = f"Dag id {dag_id} not found"
        raise DagNotFound(error_message)
    if task_id and not dag.has_task(task_id):
        error_message = f"Task {task_id} not found in dag {dag_id}"
        raise TaskNotFound(error_message)
    return dag


def check_and_get_dagrun(dag: DagModel, execution_date: datetime) -> DagRun:
    """Get DagRun object and check that it exists."""
    dagrun = dag.get_dagrun(execution_date=execution_date)
    if not dagrun:
        error_message = f"Dag Run for date {execution_date} not found in dag {dag.dag_id}"
        raise DagRunNotFound(error_message)
    return dagrun
