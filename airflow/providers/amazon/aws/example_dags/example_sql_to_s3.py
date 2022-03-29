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


import os
from datetime import datetime

from airflow import models
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")
SQL_QUERY = os.environ.get("SQL_QUERY", "SHOW tables")

with models.DAG(
    "example_sql_to_s3",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_transfer_sql_to_s3]
    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_task",
        sql_conn_id="mysql_default",
        query=SQL_QUERY,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        replace=True,
    )
    # [END howto_transfer_sql_to_s3]
