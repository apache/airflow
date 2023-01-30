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
This is a basic example DAG for using `SalesforceToS3Operator` to retrieve Salesforce account
data and upload it to an Amazon S3 bucket.
"""
from __future__ import annotations

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator

S3_BUCKET_NAME = getenv("S3_BUCKET_NAME", "s3_bucket_name")
S3_KEY = getenv("S3_KEY", "s3_filename")


with DAG(
    dag_id="example_salesforce_to_s3",
    start_date=datetime(2021, 7, 8),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_transfer_salesforce_to_s3]
    upload_salesforce_data_to_s3 = SalesforceToS3Operator(
        task_id="upload_salesforce_to_s3",
        salesforce_query="SELECT AccountNumber, Name FROM Account",
        s3_bucket_name=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        salesforce_conn_id="salesforce",
        replace=True,
    )
    # [END howto_transfer_salesforce_to_s3]
