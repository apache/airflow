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
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.snowflake_to_slack import SnowflakeToSlackOperator

SNOWFLAKE_CONN_ID = 'my_snowflake_conn'
SLACK_CONN_ID = 'my_slack_conn'
# TODO: should be able to rely on connection's schema, but currently param required by S3ToSnowflakeTransfer
SNOWFLAKE_SCHEMA = 'schema_name'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'warehouse_name'
SNOWFLAKE_DATABASE = 'database_name'
SNOWFLAKE_ROLE = 'role_name'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table'
S3_FILE_PATH = '</path/to/file/sample_file.csv'

# SQL commands
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(0, 10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
SNOWFLAKE_SLACK_SQL = f"SELECT name, id FROM {SNOWFLAKE_SAMPLE_TABLE} LIMIT 10;"
SNOWFLAKE_SLACK_MESSAGE = (
    "Results in an ASCII table:\n```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```"
)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake"

# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={'snowflake_conn_id': SNOWFLAKE_CONN_ID},
    tags=['example'],
    catchup=False,
) as dag:
    # [START snowflake_example_dag]
    snowflake_op_sql_str = SnowflakeOperator(
        task_id='snowflake_op_sql_str',
        sql=CREATE_TABLE_SQL_STRING,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_with_params = SnowflakeOperator(
        task_id='snowflake_op_with_params',
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )

    snowflake_op_sql_list = SnowflakeOperator(task_id='snowflake_op_sql_list', sql=SQL_LIST)

    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id='snowflake_op_sql_multiple_stmts',
        sql=SQL_MULTIPLE_STMTS,
    )

    snowflake_op_template_file = SnowflakeOperator(
        task_id='snowflake_op_template_file',
        sql='/path/to/sql/<filename>.sql',
    )

    # [END howto_operator_snowflake]

    # [START howto_operator_s3_to_snowflake]

    copy_into_table = S3ToSnowflakeOperator(
        task_id='copy_into_table',
        s3_keys=[S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ';')",
        pattern=".*[.]csv",
    )

    # [END howto_operator_s3_to_snowflake]

    # [START howto_operator_snowflake_to_slack]

    slack_report = SnowflakeToSlackOperator(
        task_id="slack_report",
        sql=SNOWFLAKE_SLACK_SQL,
        slack_message=SNOWFLAKE_SLACK_MESSAGE,
        slack_conn_id=SLACK_CONN_ID,
    )

    # [END howto_operator_snowflake_to_slack]

    (
        snowflake_op_sql_str
        >> [
            snowflake_op_with_params,
            snowflake_op_sql_list,
            snowflake_op_template_file,
            copy_into_table,
            snowflake_op_sql_multiple_stmts,
        ]
        >> slack_report
    )
    # [END snowflake_example_dag]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
