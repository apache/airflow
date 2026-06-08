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
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeSqlApiOperator,
    SnowflakeValueCheckOperator,
)

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_SAMPLE_TABLE = "sample_table"
SNOWFLAKE_CHECK_TABLE = "sample_check_table"

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
CREATE_CHECK_TABLE_SQL_STRING = f"""
CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_CHECK_TABLE} (
    ds DATE,
    value INT
);
"""
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
SQL_CHECK_TABLE_INSERT = f"""
INSERT INTO {SNOWFLAKE_CHECK_TABLE} (ds, value)
VALUES
    (TO_DATE('{{{{ ds }}}}'), 4),
    (TO_DATE('{{{{ macros.ds_add(ds, -1) }}}}'), 4)
"""
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake"


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID, "conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_snowflake]
    snowflake_op_sql_str = SQLExecuteQueryOperator(
        task_id="snowflake_op_sql_str", sql=CREATE_TABLE_SQL_STRING
    )

    snowflake_op_with_params = SQLExecuteQueryOperator(
        task_id="snowflake_op_with_params",
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
    )

    snowflake_op_sql_list = SQLExecuteQueryOperator(task_id="snowflake_op_sql_list", sql=SQL_LIST)

    snowflake_op_sql_multiple_stmts = SQLExecuteQueryOperator(
        task_id="snowflake_op_sql_multiple_stmts",
        sql=SQL_MULTIPLE_STMTS,
        split_statements=True,
    )

    snowflake_op_template_file = SQLExecuteQueryOperator(
        task_id="snowflake_op_template_file",
        sql="example_snowflake_snowflake_op_template_file.sql",
    )

    # Create and populate a small dataset for the data quality operator examples.
    # We insert one row for `ds` and one for `ds - 1`, each with value = 4.
    create_check_table = SQLExecuteQueryOperator(
        task_id="create_check_table",
        sql=CREATE_CHECK_TABLE_SQL_STRING,
    )

    populate_check_table = SQLExecuteQueryOperator(
        task_id="populate_check_table",
        sql=SQL_CHECK_TABLE_INSERT,
    )

    # [END howto_operator_snowflake]

    # [START howto_operator_snowflake_check]
    snowflake_check = SnowflakeCheckOperator(
        task_id="snowflake_check",
        sql=f"SELECT COUNT(*) FROM {SNOWFLAKE_CHECK_TABLE} WHERE ds = TO_DATE('{{{{ ds }}}}')",
    )
    # [END howto_operator_snowflake_check]

    # [START howto_operator_snowflake_value_check]
    snowflake_value_check = SnowflakeValueCheckOperator(
        task_id="snowflake_value_check",
        sql=f"SELECT SUM(value) FROM {SNOWFLAKE_CHECK_TABLE} WHERE ds = TO_DATE('{{{{ ds }}}}')",
        pass_value=4,
    )
    # [END howto_operator_snowflake_value_check]

    # [START howto_operator_snowflake_interval_check]
    snowflake_interval_check = SnowflakeIntervalCheckOperator(
        task_id="snowflake_interval_check",
        table=SNOWFLAKE_CHECK_TABLE,
        metrics_thresholds={"COUNT(*)": 1.5},
        days_back=1,
    )
    # [END howto_operator_snowflake_interval_check]

    # [START howto_snowflake_sql_api_operator]
    snowflake_sql_api_op_sql_multiple_stmt = SnowflakeSqlApiOperator(
        task_id="snowflake_op_sql_multiple_stmt",
        sql=SQL_MULTIPLE_STMTS,
        statement_count=len(SQL_LIST),
    )
    # [END howto_snowflake_sql_api_operator]

    (
        snowflake_op_sql_str
        >> [
            snowflake_op_with_params,
            snowflake_op_sql_list,
            snowflake_op_template_file,
            snowflake_op_sql_multiple_stmts,
            snowflake_sql_api_op_sql_multiple_stmt,
        ]
    )

    snowflake_op_sql_str >> create_check_table
    (
        create_check_table
        >> populate_check_table
        >> [
            snowflake_check,
            snowflake_value_check,
            snowflake_interval_check,
        ]
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
