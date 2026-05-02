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
from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_DATE = datetime(2024, 1, 1)

with DAG(
    dag_id="test_dag_test_trigger_parent",
    schedule=None,
    start_date=DEFAULT_DATE,
    is_paused_upon_creation=False,
) as parent_dag:
    TriggerDagRunOperator(
        task_id="trigger_target_dag",
        trigger_dag_id="test_dag_test_trigger_target",
    )

with DAG(
    dag_id="test_dag_test_trigger_target",
    schedule=None,
    start_date=DEFAULT_DATE,
    is_paused_upon_creation=False,
) as target_dag:
    EmptyOperator(task_id="target_task")
