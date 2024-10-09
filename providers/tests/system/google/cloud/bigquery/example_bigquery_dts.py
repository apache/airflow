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
from pathlib import Path
from typing import cast

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "gcp_bigquery_dts"

BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"

FILE_NAME = "us-states.csv"
CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / FILE_NAME)
BUCKET_URI = f"gs://{BUCKET_NAME}/{FILE_NAME}"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
DTS_BQ_TABLE = "DTS_BQ_TABLE"

# [START howto_bigquery_dts_create_args]

# In the case of Airflow, the customer needs to create a transfer
# config with the automatic scheduling disabled and then trigger
# a transfer run using a specialized Airflow operator
TRANSFER_CONFIG = {
    "destination_dataset_id": DATASET_NAME,
    "display_name": "test data transfer",
    "data_source_id": "google_cloud_storage",
    "schedule_options": {"disable_auto_scheduling": True},
    "params": {
        "field_delimiter": ",",
        "max_bad_records": "0",
        "skip_leading_rows": "1",
        "data_path_template": BUCKET_URI,
        "destination_table_name_template": DTS_BQ_TABLE,
        "file_format": "CSV",
    },
}

# [END howto_bigquery_dts_create_args]

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=DTS_BQ_TABLE,
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # [START howto_bigquery_create_data_transfer]
    gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
        transfer_config=TRANSFER_CONFIG,
        project_id=PROJECT_ID,
        task_id="gcp_bigquery_create_transfer",
    )

    transfer_config_id = cast(str, XComArg(gcp_bigquery_create_transfer, key="transfer_config_id"))
    # [END howto_bigquery_create_data_transfer]

    # [START howto_bigquery_start_transfer]
    gcp_bigquery_start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
        task_id="gcp_bigquery_start_transfer",
        project_id=PROJECT_ID,
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
    gcp_bigquery_delete_transfer.trigger_rule = TriggerRule.ALL_DONE

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    # Task dependencies created via `XComArgs`:
    #   gcp_bigquery_create_transfer >> gcp_bigquery_start_transfer
    #   gcp_bigquery_create_transfer >> gcp_run_sensor
    #   gcp_bigquery_start_transfer >> gcp_run_sensor
    #   gcp_bigquery_create_transfer >> gcp_bigquery_delete_transfer

    chain(
        # TEST SETUP
        create_bucket,
        upload_file,
        create_dataset,
        create_table,
        # TEST BODY
        gcp_bigquery_create_transfer,
        gcp_bigquery_start_transfer,
        gcp_run_sensor,
        gcp_bigquery_delete_transfer,
        # TEST TEARDOWN
        delete_dataset,
        delete_bucket,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
