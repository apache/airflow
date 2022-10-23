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
Example use of Snowflake related operators.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator

SNOWFLAKE_CONN_ID = 'my_snowflake_conn'
SLACK_CONN_ID = 'my_slack_conn'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table'

# SQL commands
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake_to_slack"

# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['example'],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_snowflake_to_slack]

    slack_report = SnowflakeToSlackOperator(
        task_id="slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message=SNOWFLAKE_SLACK_MESSAGE,
        slack_conn_id=SLACK_CONN_ID,
    )

    # [END howto_operator_snowflake_to_slack]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
