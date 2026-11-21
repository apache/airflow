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
Ask a Databricks Genie space a question as a standalone, resumable task.

Configure DATABRICKS_GENIE_SPACE_ID and the databricks_default connection before running this system test.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG, task
from airflow.providers.databricks.operators.genie import DatabricksGenieOperator

DAG_ID = "example_databricks_genie"
SPACE_ID = os.environ.get("DATABRICKS_GENIE_SPACE_ID", "your-genie-space-id")
CONN_ID = os.environ.get("DATABRICKS_GENIE_CONN_ID", "databricks_default")
QUESTION = "How many orders were placed last week?"


@task
def verify_result(result: dict) -> None:
    """Verify that Genie answered and returned its identifiers."""
    assert result["status"] == "COMPLETED"
    assert result["space_id"] == SPACE_ID
    assert result["conversation_id"]
    assert result["message_id"]


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_genie]
    ask = DatabricksGenieOperator(
        task_id="ask_genie",
        space_id=SPACE_ID,
        question=QUESTION,
        databricks_conn_id=CONN_ID,
        timeout=600,
    )
    # [END howto_operator_databricks_genie]

    # [START howto_operator_databricks_genie_deferrable]
    follow_up = DatabricksGenieOperator(
        task_id="ask_genie_follow_up",
        space_id=SPACE_ID,
        question="Break that down by region.",
        conversation_id="{{ ti.xcom_pull(task_ids='ask_genie')['conversation_id'] }}",
        databricks_conn_id=CONN_ID,
        deferrable=True,
        timeout=600,
    )
    # [END howto_operator_databricks_genie_deferrable]

    ask >> verify_result.override(task_id="verify_ask")(ask.output) >> follow_up
    follow_up >> verify_result.override(task_id="verify_follow_up")(follow_up.output)

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
