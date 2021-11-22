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

from datetime import datetime

from airflow import models
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator

SQL = "select 1 as my_col"
NEW_SPREADSHEET_ID = "123"

with models.DAG(
    "example_sql_to_sheets",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,  # Override to match your needs
    catchup=False,
    tags=["example"],
) as dag:

    # [START upload_sql_to_sheets]
    upload_gcs_to_sheet = SQLToGoogleSheetsOperator(
        task_id="upload_sql_to_sheet",
        sql=SQL,
        sql_conn_id="database_conn_id",
        spreadsheet_id=NEW_SPREADSHEET_ID,
    )
    # [END upload_sql_to_sheets]
