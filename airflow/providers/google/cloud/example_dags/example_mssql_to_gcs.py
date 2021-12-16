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
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator

GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "example-airflow")
FILENAME = 'test_file'

SQL_QUERY = "USE airflow SELECT * FROM Country;"

with models.DAG(
    'example_mssql_to_gcs',
    schedule_interval='@once',
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_mssql_to_gcs]
    upload = MSSQLToGCSOperator(
        task_id='mssql_to_gcs',
        mssql_conn_id='airflow_mssql',
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format='csv',
    )
    # [END howto_operator_mssql_to_gcs]
