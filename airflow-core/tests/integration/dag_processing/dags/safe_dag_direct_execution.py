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
Test DAG file for direct execution outside DagBag context.

This file tests that safe_dag works correctly when a DAG file is executed
directly (e.g., python safe_dag_direct_execution.py) rather than being
processed by DagBag.
"""

from __future__ import annotations

from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.dag import safe_dag

created_dags = []

with safe_dag():
    direct_execution_dag = DAG(
        "direct_execution_dag",
        start_date=datetime(2024, 1, 1),
        description="DAG created during direct execution",
    )
    direct_task = BashOperator(
        task_id="direct_task", bash_command="echo 'Hello from direct execution'", dag=direct_execution_dag
    )
    created_dags.append(direct_execution_dag)
