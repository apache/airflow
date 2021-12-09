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
Example Airflow DAG showing usage of impersonation chain argument for Google BigQuery service.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset_transfer")
IMPERSONATION_CHAIN = f"impersonated_account@{PROJECT_ID}.iam.gserviceaccount.com"

with models.DAG(
    "example_bigquery_to_bigquery",
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        location="us-central1",
        impersonation_chain=IMPERSONATION_CHAIN,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", 
        dataset_id=DATASET_NAME, 
        location="us-central1",
        impersonation_chain=IMPERSONATION_CHAIN,
        delete_contents=True
    )

    create_dataset >> delete_dataset
