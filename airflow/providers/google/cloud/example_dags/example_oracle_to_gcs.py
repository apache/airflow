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

from airflow import models
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.utils import dates

GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET", "example-airflow-oracle-gcs")
FILENAME = 'test_file'

SQL_QUERY = "SELECT * from test_table"

with models.DAG(
    'example_oracle_to_gcs',
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_oracle_to_gcs]
    upload = OracleToGCSOperator(
        task_id='oracle_to_gcs', sql=SQL_QUERY, bucket=GCS_BUCKET, filename=FILENAME, export_format='csv'
    )
    # [END howto_operator_oracle_to_gcs]
