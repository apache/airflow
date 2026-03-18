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
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
# TODO: should be able to rely on connection's schema,
#  but currently param required by CopyFromExternalStageToSnowflakeOperator
SNOWFLAKE_STAGE = "stage_name"
SNOWFLAKE_SAMPLE_TABLE = "sample_table"
S3_FILE_PATH = "</path/to/file/sample_file.csv"

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_s3_to_snowflake"

# [START howto_operator_snowflake]

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_s3_copy_into_snowflake]
    copy_into_table = CopyFromExternalStageToSnowflakeOperator(
        task_id="copy_into_table",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        files=[S3_FILE_PATH],
        table=SNOWFLAKE_SAMPLE_TABLE,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ';')",
        pattern=".*[.]csv",
    )
    # [END howto_operator_s3_copy_into_snowflake]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
