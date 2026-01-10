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
Example Airflow DAG that shows how to use DisplayVideo.

In order to run this test, make sure you followed steps:
1. In your GCP project, create a service account that will be used to operate on Google Ads.
The name should be in format `display-video-service-account@{PROJECT_ID}.iam.gserviceaccount.com`
2. Generate a key for this service account and store it in the Secret Manager
under the name `google_display_video_service_account_key`.
3. Give this service account Editor permissions.
4. Make sure Google Display Video API is enabled in your GCP project.
5. Login to https://displayvideo.google.com/
6. In the Users section add your GCP service account to the list.
7. Store values of your advertiser id and GMP partner id to Secret Manager under names `google_display_video_advertiser_id`
and `google_display_video_gmp_partner_id`.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.common.utils.get_secret import get_secret
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
)
from airflow.providers.google.marketing_platform.sensors.display_video import (
    GoogleDisplayVideo360GetSDFDownloadOperationSensor,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google.gcp_api_client_helpers import (
    create_airflow_connection,
    delete_airflow_connection,
)

DAG_ID = "display_video"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
OBJECT_NAME = "files/report.csv"
PATH_TO_UPLOAD_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.csv")
BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]

IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

CONNECTION_TYPE = "google_cloud_platform"
CONN_ID = "google_display_video_default"

DISPLAY_VIDEO_ADVERTISER_ID = "google_display_video_advertiser_id"
DISPLAY_VIDEO_GMP_PARTNER_ID = "google_display_video_gmp_partner_id"
DISPLAY_VIDEO_SERVICE_ACCOUNT_KEY = "google_display_video_service_account_key"

# [START howto_display_video_env_variables]
ADVERTISER_ID = "{{ task_instance.xcom_pull('get_display_video_advertiser_id') }}"
GMP_PARTNER_ID = "{{ task_instance.xcom_pull('get_display_video_gmp_partner_id') }}"
SDF_VERSION = "SDF_VERSION_8_1"
BQ_DATASET = f"bq_dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")
ENTITY_TYPE = "CAMPAIGN"
ERF_SOURCE_OBJECT = GoogleDisplayVideo360Hook.erf_uri(GMP_PARTNER_ID, ENTITY_TYPE)

CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST: dict = {
    "version": SDF_VERSION,
    "advertiserId": ADVERTISER_ID,
    "inventorySourceFilter": {"inventorySourceIds": []},
}

# [END howto_display_video_env_variables]


with DAG(
    "display_video_sdf",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "display_video_sdf"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_display_video_advertiser_id():
        return get_secret(secret_id=DISPLAY_VIDEO_ADVERTISER_ID).strip()

    get_display_video_advertiser_id_task = get_display_video_advertiser_id()

    @task
    def get_display_video_gmp_partner_id():
        return get_secret(secret_id=DISPLAY_VIDEO_GMP_PARTNER_ID).strip()

    get_display_video_gmp_partner_id_task = get_display_video_gmp_partner_id()

    @task
    def get_display_video_service_account_key():
        return get_secret(secret_id=DISPLAY_VIDEO_SERVICE_ACCOUNT_KEY)

    get_display_video_service_account_key_task = get_display_video_service_account_key()

    @task
    def create_connection_display_video(connection_id: str, key) -> None:
        conn_extra_json = json.dumps(
            {
                "keyfile_dict": key,
                "project": PROJECT_ID,
                "scope": "https://www.googleapis.com/auth/display-video, https://www.googleapis.com/auth/cloud-platform",
            }
        )
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf={"conn_type": CONNECTION_TYPE, "extra": conn_extra_json},
            is_composer=IS_COMPOSER,
        )

    create_connection_display_video_task = create_connection_display_video(
        connection_id=CONN_ID, key=get_display_video_service_account_key_task
    )

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID, gcp_conn_id=CONN_ID
    )

    # [START howto_google_display_video_create_sdf_download_task_operator]
    create_sdf_download_task = GoogleDisplayVideo360CreateSDFDownloadTaskOperator(
        task_id="create_sdf_download_task",
        body_request=CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST,
        gcp_conn_id=CONN_ID,
    )
    operation_name = '{{ task_instance.xcom_pull("create_sdf_download_task")["name"] }}'
    # [END howto_google_display_video_create_sdf_download_task_operator]

    # [START howto_google_display_video_wait_for_operation_sensor]
    wait_for_operation = GoogleDisplayVideo360GetSDFDownloadOperationSensor(
        task_id="wait_for_operation", operation_name=operation_name, gcp_conn_id=CONN_ID
    )
    # [END howto_google_display_video_wait_for_operation_sensor]

    # [START howto_google_display_video_save_sdf_in_gcs_operator]
    save_sdf_in_gcs = GoogleDisplayVideo360SDFtoGCSOperator(
        task_id="save_sdf_in_gcs",
        operation_name=operation_name,
        bucket_name=BUCKET_NAME,
        object_name=BUCKET_FILE_LOCATION,
        gzip=False,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_google_display_video_save_sdf_in_gcs_operator]

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=BQ_DATASET, gcp_conn_id=CONN_ID
    )

    # [START howto_google_display_video_gcs_to_big_query_operator]
    upload_sdf_to_big_query = GCSToBigQueryOperator(
        task_id="upload_sdf_to_big_query",
        bucket=BUCKET_NAME,
        source_objects=[PATH_TO_UPLOAD_FILE],
        destination_project_dataset_table=f"{BQ_DATASET}.gcs_to_bq_table",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=CONN_ID,
    )
    # [END howto_google_display_video_gcs_to_big_query_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        gcp_conn_id=CONN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection_task")
    def delete_connection_display_video(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection_display_video(connection_id=CONN_ID)

    (
        # TEST SETUP
        [
            get_display_video_advertiser_id_task,
            get_display_video_gmp_partner_id_task,
            get_display_video_service_account_key_task,
        ]
        >> create_connection_display_video_task
        >> create_bucket
        >> create_dataset
        # TEST BODY
        >> create_sdf_download_task
        >> wait_for_operation
        >> save_sdf_in_gcs
        >> upload_sdf_to_big_query
        # TEST TEARDOWN
        >> delete_bucket
        >> delete_connection_task
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
