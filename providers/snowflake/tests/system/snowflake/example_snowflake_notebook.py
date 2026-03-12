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
Example use of SnowflakeNotebookOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake_notebook import SnowflakeNotebookOperator

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_NOTEBOOK = os.environ.get("SNOWFLAKE_NOTEBOOK", "MY_DB.MY_SCHEMA.MY_NOTEBOOK")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_snowflake_notebook"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_snowflake_notebook]
    execute_notebook = SnowflakeNotebookOperator(
        task_id="execute_notebook",
        notebook=SNOWFLAKE_NOTEBOOK,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )
    # [END howto_operator_snowflake_notebook]

    # [START howto_operator_snowflake_notebook_with_params]
    execute_notebook_with_params = SnowflakeNotebookOperator(
        task_id="execute_notebook_with_params",
        notebook=SNOWFLAKE_NOTEBOOK,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        parameters=["param1", "target_db=PROD"],
    )
    # [END howto_operator_snowflake_notebook_with_params]

    # [START howto_operator_snowflake_notebook_deferrable]
    execute_notebook_deferrable = SnowflakeNotebookOperator(
        task_id="execute_notebook_deferrable",
        notebook=SNOWFLAKE_NOTEBOOK,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        deferrable=True,
    )
    # [END howto_operator_snowflake_notebook_deferrable]

    execute_notebook >> execute_notebook_with_params >> execute_notebook_deferrable


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
