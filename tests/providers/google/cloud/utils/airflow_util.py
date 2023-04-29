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
from __future__ import annotations

from unittest import mock

import pendulum

from airflow.models import DAG, Connection
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.types import DagRunType


def get_dag_run(dag_id: str = "test_dag_id", run_id: str = "test_dag_id") -> DagRun:
    dag_run = DagRun(
        dag_id=dag_id, run_type="manual", execution_date=timezone.datetime(2022, 1, 1), run_id=run_id
    )
    return dag_run


def get_task_instance(task: BaseOperator) -> TaskInstance:
    return TaskInstance(task, timezone.datetime(2022, 1, 1))


def get_conn() -> Connection:
    return Connection(
        conn_id="test_conn",
        extra={},
    )


def create_context(task, dag=None):
    if dag is None:
        dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )

    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
        "dag_run": dag_run,
        "execution_date": execution_date,
        "data_interval_end": execution_date,
        "logical_date": execution_date,
    }
