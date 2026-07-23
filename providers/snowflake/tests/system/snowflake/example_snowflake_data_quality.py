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
Example use of Snowflake data quality check operators.

Demonstrates SnowflakeCheckOperator, SnowflakeValueCheckOperator,
and SnowflakeIntervalCheckOperator for common data quality use cases.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeValueCheckOperator,
)

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "public")
SNOWFLAKE_DATA_QUALITY_TABLE = "data_quality_example"

# SQL to set up a sample table with data for the check operators
SETUP_TABLE_SQL = f"""
CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_DATA_QUALITY_TABLE} (
    id INT,
    name VARCHAR(250),
    amount DECIMAL(10, 2),
    ds DATE
);
"""

POPULATE_TABLE_SQL = f"""
INSERT INTO {SNOWFLAKE_DATA_QUALITY_TABLE} (id, name, amount, ds) VALUES
    (1, 'Alice', 100.00, CURRENT_DATE),
    (2, 'Bob', 200.50, CURRENT_DATE),
    (3, 'Charlie', 150.75, CURRENT_DATE),
    (4, 'Alice', 100.00, DATEADD(DAY, -7, CURRENT_DATE)),
    (5, 'Bob', 195.00, DATEADD(DAY, -7, CURRENT_DATE)),
    (6, 'Charlie', 145.25, DATEADD(DAY, -7, CURRENT_DATE));
"""

TEARDOWN_TABLE_SQL = f"DROP TABLE IF EXISTS {SNOWFLAKE_DATA_QUALITY_TABLE};"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake_data_quality"


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    setup_table = SQLExecuteQueryOperator(
        task_id="setup_table",
        sql=SETUP_TABLE_SQL,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    populate_table = SQLExecuteQueryOperator(
        task_id="populate_table",
        sql=POPULATE_TABLE_SQL,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # [START howto_operator_snowflake_check]
    snowflake_check = SnowflakeCheckOperator(
        task_id="snowflake_check",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {SNOWFLAKE_DATA_QUALITY_TABLE}",
    )
    # [END howto_operator_snowflake_check]

    # [START howto_operator_snowflake_value_check]
    snowflake_value_check = SnowflakeValueCheckOperator(
        task_id="snowflake_value_check",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {SNOWFLAKE_DATA_QUALITY_TABLE}",
        pass_value=6,
        tolerance=0.0,
    )
    # [END howto_operator_snowflake_value_check]

    # [START howto_operator_snowflake_interval_check]
    snowflake_interval_check = SnowflakeIntervalCheckOperator(
        task_id="snowflake_interval_check",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        table=SNOWFLAKE_DATA_QUALITY_TABLE,
        metrics_thresholds={"COUNT(*)": 1.5, "SUM(amount)": 1.5},
        date_filter_column="ds",
        days_back=-7,
    )
    # [END howto_operator_snowflake_interval_check]

    teardown_table = SQLExecuteQueryOperator(
        task_id="teardown_table",
        sql=TEARDOWN_TABLE_SQL,
        conn_id=SNOWFLAKE_CONN_ID,
        trigger_rule="all_done",
    )

    (
        setup_table
        >> populate_table
        >> [snowflake_check, snowflake_value_check, snowflake_interval_check]
        >> teardown_table
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
