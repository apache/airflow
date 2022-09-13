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
Example Airflow DAG that creates and deletes Bigquery data transfer configurations.
"""
from __future__ import annotations

import os
import time
from datetime import datetime
from typing import cast

from airflow import models
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_URI = os.environ.get("GCP_DTS_BUCKET_URI", "gs://INVALID BUCKET NAME/bank-marketing.csv")
GCP_DTS_BQ_DATASET = os.environ.get("GCP_DTS_BQ_DATASET", "test_dts")
GCP_DTS_BQ_TABLE = os.environ.get("GCP_DTS_BQ_TABLE", "GCS_Test")

# [START howto_bigquery_dts_create_args]

# In the case of Airflow, the customer needs to create a transfer
# config with the automatic scheduling disabled and then trigger
# a transfer run using a specialized Airflow operator
schedule_options = {"disable_auto_scheduling": True}

PARAMS = {
    "field_delimiter": ",",
    "max_bad_records": "0",
    "skip_leading_rows": "1",
    "data_path_template": BUCKET_URI,
    "destination_table_name_template": GCP_DTS_BQ_TABLE,
    "file_format": "CSV",
}

TRANSFER_CONFIG = {
    "destination_dataset_id": GCP_DTS_BQ_DATASET,
    "display_name": "GCS Test Config",
    "data_source_id": "google_cloud_storage",
    "schedule_options": schedule_options,
    "params": PARAMS,
}

# [END howto_bigquery_dts_create_args]

with models.DAG(
    "example_gcp_bigquery_dts",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_bigquery_create_data_transfer]
    gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
        transfer_config=TRANSFER_CONFIG,
        project_id=GCP_PROJECT_ID,
        task_id="gcp_bigquery_create_transfer",
    )

    transfer_config_id = cast(str, XComArg(gcp_bigquery_create_transfer, key="transfer_config_id"))
    # [END howto_bigquery_create_data_transfer]

    # [START howto_bigquery_start_transfer]
    gcp_bigquery_start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="gcp_bigquery_start_transfer",
        transfer_config_id=transfer_config_id,
        requested_run_time={"seconds": int(time.time() + 60)},
    )
    # [END howto_bigquery_start_transfer]

    # [START howto_bigquery_dts_sensor]
    gcp_run_sensor = BigQueryDataTransferServiceTransferRunSensor(
        task_id="gcp_run_sensor",
        transfer_config_id=transfer_config_id,
        run_id=cast(str, XComArg(gcp_bigquery_start_transfer, key="run_id")),
        expected_statuses={"SUCCEEDED"},
    )
    # [END howto_bigquery_dts_sensor]

    # [START howto_bigquery_delete_data_transfer]
    gcp_bigquery_delete_transfer = BigQueryDeleteDataTransferConfigOperator(
        transfer_config_id=transfer_config_id, task_id="gcp_bigquery_delete_transfer"
    )
    # [END howto_bigquery_delete_data_transfer]

    gcp_run_sensor >> gcp_bigquery_delete_transfer

    # Task dependencies created via `XComArgs`:
    #   gcp_bigquery_create_transfer >> gcp_bigquery_start_transfer
    #   gcp_bigquery_create_transfer >> gcp_run_sensor
    #   gcp_bigquery_start_transfer >> gcp_run_sensor
    #   gcp_bigquery_create_transfer >> gcp_bigquery_delete_transfer
