# -*- coding: utf-8 -*-
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
import os
import time

from google.cloud.bigquery_datatransfer_v1.types import TransferConfig
from google.protobuf.json_format import ParseDict

from airflow import models
from airflow.gcp.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator, BigQueryDataTransferServiceStartTransferRunsOperator,
)
from airflow.gcp.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET_URI = os.environ.get(
    "GCP_DTS_BUCKET_URI", "gs://cloud-ml-tables-data/bank-marketing.csv"
)
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

TRANSFER_CONFIG = ParseDict(
    {
        "destination_dataset_id": GCP_DTS_BQ_DATASET,
        "display_name": "GCS Test Config",
        "data_source_id": "google_cloud_storage",
        "schedule_options": schedule_options,
        "params": PARAMS,
    },
    TransferConfig(),
)

# [END howto_bigquery_dts_create_args]

default_args = {"start_date": days_ago(1)}

with models.DAG(
    "example_gcp_bigquery_dts",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:
    # [START howto_bigquery_create_data_transfer]
    gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
        transfer_config=TRANSFER_CONFIG,
        project_id=GCP_PROJECT_ID,
        task_id="gcp_bigquery_create_transfer",
    )

    transfer_config_id = (
        "{{ task_instance.xcom_pull('gcp_bigquery_create_transfer', "
        "key='transfer_config_id') }}"
    )
    # [END howto_bigquery_create_data_transfer]

    # [START howto_bigquery_start_transfer]
    gcp_bigquery_start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="gcp_bigquery_start_transfer",
        transfer_config_id=transfer_config_id,
        requested_run_time={"seconds": int(time.time() + 60)},
    )
    run_id = (
        "{{ task_instance.xcom_pull('gcp_bigquery_start_transfer', " "key='run_id') }}"
    )
    # [END howto_bigquery_start_transfer]

    # [START howto_bigquery_dts_sensor]
    gcp_run_sensor = BigQueryDataTransferServiceTransferRunSensor(
        task_id="gcp_run_sensor",
        transfer_config_id=transfer_config_id,
        run_id=run_id,
        expected_statuses={"SUCCEEDED"},
    )
    # [END howto_bigquery_dts_sensor]

    # [START howto_bigquery_delete_data_transfer]
    gcp_bigquery_delete_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        transfer_config_id=transfer_config_id, task_id="gcp_bigquery_delete_transfer"
    )
    # [END howto_bigquery_delete_data_transfer]

    (
        gcp_bigquery_create_transfer  # noqa
        >> gcp_bigquery_start_transfer  # noqa
        >> gcp_run_sensor  # noqa
        >> gcp_bigquery_delete_transfer  # noqa
    )
