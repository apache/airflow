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
This is a basic example DAG for using `SalesforceToS3Operator` to retrieve Salesforce customer
data, upload to an S3 data lake, and copy into a Snowflake data warehouse.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.transfers.salesforce_to_s3 import SalesforceToS3Operator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


BASE_PATH = "salesforce/customers"
DATE_PREFIXES = "{{ execution_date.strftime('%Y/%m/%d') }}"
FILE_NAME = "customer_extract_{{ ds_nodash }}.json"

with DAG(
    dag_id="example_salesforce_to_s3_transfer",
    schedule_interval="@daily",
    start_date=datetime(2021, 7, 8),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_salesforce_to_s3_transfer]
    upload_salesforce_data_to_s3 = SalesforceToS3Operator(
        task_id="upload_salesforce_data_to_s3",
        query="SELECT Id, Name, Company, Phone, Email, Region, CreatedDate, LastModifiedDate, IsActive FROM Customers",
        s3_bucket_name="data-lake-raw",
        s3_key=f"{BASE_PATH}/{DATE_PREFIXES}/{FILE_NAME}",
        salesforce_conn_id="salesforce",
        export_format="json",
        aws_conn_id="s3",
        replace=True,
    )
    # [END howto_operator_salesforce_to_s3_transfer]

    copy_from_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="copy_from_s3_to_snowflake",
        snowflake_conn_id="snowflake",
        prefix=f"{BASE_PATH}/{DATE_PREFIXES}/",
        table="customers",
        schema="ingest",
        stage="s3_data_lake_raw",
        file_format="(type = 'JSON')",
    )

    upload_salesforce_data_to_s3 >> copy_from_s3_to_snowflake
