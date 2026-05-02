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
Integration-test DAG for @apache-airflow/ts-sdk (PR #1 minimal-worker scope).

Exposes a single-task DAG on queue "core-integration-test" so the core
package's integration test can:
  1. Register a handler for task_id "core_smoke"
  2. Start a worker on queue "core-integration-test"
  3. Trigger this DAG
  4. Verify the task reaches state=success

The task body is a BashOperator purely so the DAG is authoring-valid on
the Python side. EdgeExecutor dispatches based on queue name; the actual
work is done by whatever handler the TS worker has registered for the
matching task_id.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="core_integration",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ts-sdk-core", "integration"],
    description="Integration-test DAG for @apache-airflow/ts-sdk minimal worker.",
) as dag:
    core_smoke = BashOperator(
        task_id="core_smoke",
        queue="core-integration-test",
        bash_command='echo "placeholder — real handler runs on TS worker"',
    )
