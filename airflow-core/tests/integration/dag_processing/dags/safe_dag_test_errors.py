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

from airflow.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.dag import safe_dag

# Successful DAG
with safe_dag():
    successful_dag = DAG("successful_dag", start_date=datetime(2024, 1, 1))
    successful_task = BashOperator(
        task_id="successful_task", bash_command="echo hello world", dag=successful_dag
    )

# DAG will fail with missing required parameter - bash_command
with safe_dag():
    failing_dag = DAG("failing_dag", start_date=datetime(2024, 1, 1))
    failing_task = BashOperator(task_id="failing_task", dag=failing_dag)

# DAG will fail with invalid start_date type
with safe_dag():
    another_failing_dag = DAG("another_failing_dag", start_date="invalid_date_string")  # type: ignore[arg-type]
