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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

"""
This is a basic example DAG for using `SalesforceToS3Operator` to retrieve Salesforce customer
data and upload to an S3 bucket.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator

BASE_PATH = "salesforce/customers"
FILE_NAME = "customer_daily_extract_{{ ds_nodash }}.csv"


with DAG(
    dag_id="example_salesforce_to_s3_transfer",
    schedule_interval="@daily",
    start_date=datetime(2021, 7, 8),
    catchup=False,
    default_args={"retries": 1, "aws_conn_id": "s3"},
    tags=["example"],
    default_view="graph",
) as dag:
    # [START howto_operator_salesforce_to_s3_transfer]
    upload_salesforce_data_to_s3_landing = SalesforceToS3Operator(
        task_id="upload_salesforce_data_to_s3",
        salesforce_query="SELECT Id, Name, Company, Phone, Email, LastModifiedDate, IsActive FROM Customers",
        s3_bucket_name="landing-bucket",
        s3_key=f"{BASE_PATH}/{FILE_NAME}",
        salesforce_conn_id="salesforce",
        replace=True,
    )
    # [END howto_operator_salesforce_to_s3_transfer]

    date_prefixes = "{{ execution_date.strftime('%Y/%m/%d') }}"

    store_to_s3_data_lake = S3CopyObjectOperator(
        task_id="store_to_s3_data_lake",
        source_bucket_key=upload_salesforce_data_to_s3_landing.output,
        dest_bucket_name="data_lake",
        dest_bucket_key=f"{BASE_PATH}/{date_prefixes}/{FILE_NAME}",
    )

    delete_data_from_s3_landing = S3DeleteObjectsOperator(
        task_id="delete_data_from_s3_landing",
        bucket=upload_salesforce_data_to_s3_landing.s3_bucket_name,
        keys=upload_salesforce_data_to_s3_landing.s3_key,
    )

    store_to_s3_data_lake >> delete_data_from_s3_landing

    # Task dependencies created via `XComArgs`:
    #   upload_salesforce_data_to_s3_landing >> store_to_s3_data_lake
