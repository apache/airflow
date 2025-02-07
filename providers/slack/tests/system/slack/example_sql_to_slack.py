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
Example DAG using SqlToSlackOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.slack.transfers.sql_to_slack import SqlToSlackApiFileOperator

SQL_CONN_ID = os.environ.get("SQL_CONN_ID", "postgres_default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_sql_to_slack"

with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_sql_to_slack_api_file]
    SqlToSlackApiFileOperator(
        task_id="sql_to_slack_api_file",
        sql_conn_id=SQL_CONN_ID,
        sql="SELECT 6 as multiplier, 9 as multiplicand, 42 as answer",
        slack_channels="C123456",
        slack_conn_id="slack_api_default",
        slack_filename="awesome.json.gz",
        slack_initial_comment="Awesome compressed multiline JSON.",
        df_kwargs={"orient": "records", "lines": True},
    )
    # [END howto_operator_sql_to_slack_api_file]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
