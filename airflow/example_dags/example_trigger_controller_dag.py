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
"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="example_trigger_controller_dag",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@once",
    tags=["example"],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id="example_trigger_target_dag",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )
