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
Example DAG demonstrating the SecureBashOperator.
"""
from __future__ import annotations

import datetime

from airflow import DAG
from airflow.providers.standard.operators.secure_bash import SecureBashOperator

with DAG(
    dag_id="example_secure_bash_operator",
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "security"],
) as dag:
    # [START howto_operator_secure_bash]
    # This operator is safe against command injection even if 
    # dag_run.conf contains malicious shell metacharacters.
    safe_bash_task = SecureBashOperator(
        task_id="safe_bash_task",
        bash_command='echo "Processing data for user: {{ dag_run.conf.get(\'user_id\', \'default\') }}"',
    )
    # [END howto_operator_secure_bash]
