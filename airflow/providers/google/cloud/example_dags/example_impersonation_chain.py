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
Example Airflow DAG showing usage of 'impersonation_chain' argument of Google operators.
"""
import os
import time
from urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BQ_LOCATION = "europe-north1"

DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset_operations")
LOCATION_DATASET_NAME = f"{DATASET_NAME}_location"
DATA_SAMPLE_GCS_URL = os.environ.get(
    "GCP_BIGQUERY_DATA_GCS_URL",
    "gs://cloud-samples-data/bigquery/us-states/us-states.csv",
)
IMPERSONATION_CHAIN = f"impersonated_account@{PROJECT_ID}.iam.gserviceaccount.com"

DATA_SAMPLE_GCS_URL_PARTS = urlparse(DATA_SAMPLE_GCS_URL)
DATA_SAMPLE_GCS_BUCKET_NAME = DATA_SAMPLE_GCS_URL_PARTS.netloc
DATA_SAMPLE_GCS_OBJECT_NAME = DATA_SAMPLE_GCS_URL_PARTS.path[1:]


with models.DAG(
    "example_bigquery_operations",
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=["example"],
) as dag:
    # [START howto_operator_bigquery_create_dataset]
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset", 
        dataset_id=DATASET_NAME,
        impersonation_chain=IMPERSONATION_CHAIN
    )
    # [END howto_operator_bigquery_create_dataset]


    # [START howto_operator_bigquery_delete_dataset]
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", 
        dataset_id=DATASET_NAME, 
        delete_contents=True,
        impersonation_chain=IMPERSONATION_CHAIN
    )
    # [END howto_operator_bigquery_delete_dataset]
