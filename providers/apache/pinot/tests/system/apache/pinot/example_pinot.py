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
This is an example DAG for the use of the SQLExecuteQueryOperator with Apache Pinot.
"""

from __future__ import annotations

import datetime
from textwrap import dedent

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "example_pinot"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    default_args={"conn_id": "my_pinot_conn"},
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_pinot]

    # Task: Simple query to test connection and query engine
    select_1_task = SQLExecuteQueryOperator(
        task_id="select_1",
        sql="SELECT 1",
    )

    # Task: Count total records in airlineStats (sample table)
    count_airline_stats = SQLExecuteQueryOperator(
        task_id="count_airline_stats",
        sql="SELECT COUNT(*) FROM airlineStats",
    )

    # Task: Group by Carrier and count flights
    group_by_carrier = SQLExecuteQueryOperator(
        task_id="group_by_carrier",
        sql=dedent("""
            SELECT Carrier, COUNT(*) AS flight_count
            FROM airlineStats
            GROUP BY Carrier
            ORDER BY flight_count DESC
            LIMIT 5
        """).strip(),
    )

    # [END howto_operator_pinot]

    select_1_task >> count_airline_stats >> group_by_carrier

    # Optional: watcher for system tests
    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
