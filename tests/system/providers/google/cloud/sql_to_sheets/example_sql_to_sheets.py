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
Required environment variables:
```
DB_CONNECTION = os.environ.get("DB_CONNECTION")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "test-id")
```

First, you need a db instance that is accessible from the Airflow environment.
You can, for example, create a Cloud SQL instance and connect to it from
within breeze with Cloud SQL proxy:
https://cloud.google.com/sql/docs/postgres/connect-instance-auth-proxy

# DB setup
Create db:
```
CREATE DATABASE test_db;
```

Switch to db:
```
\c test_db
```

Create table and insert some rows
```
CREATE TABLE test_table (col1 INT, col2 INT);
INSERT INTO test_table VALUES (1,2), (3,4), (5,6), (7,8);
```

# Setup connections
db connection:
In airflow UI, set one db connection, for example "postgres_default"
and make sure the "Test" at the bottom succeeds

google cloud connection:
We need additional scopes for this test
scopes: https://www.googleapis.com/auth/spreadsheets, https://www.googleapis.com/auth/cloud-platform

# Sheet
Finally, you need a Google Sheet you have access to, for testing you can
create a public sheet and get it's ID.

# Tear Down
You can delete the db with
```
DROP DATABASE test_db;
```
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DB_CONNECTION = os.environ.get("DB_CONNECTION")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "test-id")

DAG_ID = "example_sql_to_sheets"
SQL = "select col2 from test_table"

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
        sql_conn_id=DB_CONNECTION,
        database="test_db",
        spreadsheet_id=SPREADSHEET_ID,
    )
    # [END upload_sql_to_sheets]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
