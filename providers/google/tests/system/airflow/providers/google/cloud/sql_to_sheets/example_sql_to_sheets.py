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
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_sql_to_sheets"

SQL = "select 1 as my_col"
NEW_SPREADSHEET_ID = os.environ.get("NEW_SPREADSHEET_ID", "123")

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",  # Override to match your needs
    catchup=False,
    tags=["example", "sql"],
) as dag:
    # [START upload_sql_to_sheets]
    upload_gcs_to_sheet = SQLToGoogleSheetsOperator(
        task_id="upload_sql_to_sheet",
        sql=SQL,
        sql_conn_id="database_conn_id",
        spreadsheet_id=NEW_SPREADSHEET_ID,
    )
    # [END upload_sql_to_sheets]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
