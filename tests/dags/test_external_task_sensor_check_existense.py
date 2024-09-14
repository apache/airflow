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

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from tests.models import DEFAULT_DATE

with DAG(
    dag_id="test_external_task_sensor_check_existence_ext",
    schedule=None,
    start_date=DEFAULT_DATE,
) as dag1:
    EmptyOperator(task_id="empty")

with DAG(
    dag_id="test_external_task_sensor_check_existence",
    schedule=None,
    start_date=DEFAULT_DATE,
) as dag2:
    ExternalTaskSensor(
        task_id="external_task_sensor",
        external_dag_id="test_external_task_sensor_check_existence_ext",
        external_task_id="empty",
        check_existence=True,
    )
