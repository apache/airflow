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
Example Airflow DAG that demonstrates interactions with Google Cloud Transfer.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    ALREADY_EXISTING_IN_SINK,
    BUCKET_NAME,
    DESCRIPTION,
    FILTER_JOB_NAMES,
    FILTER_PROJECT_ID,
    GCS_DATA_SINK,
    GCS_DATA_SOURCE,
    PROJECT_ID,
    SCHEDULE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    START_TIME_OF_DAY,
    STATUS,
    TRANSFER_JOB,
    TRANSFER_JOB_FIELD_MASK,
    TRANSFER_OPTIONS,
    TRANSFER_SPEC,
    GcpTransferJobsStatus,
    GcpTransferOperationStatus,
)
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
    CloudDataTransferServiceGetOperationOperator,
    CloudDataTransferServiceListOperationsOperator,
    CloudDataTransferServiceRunJobOperator,
    CloudDataTransferServiceUpdateJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import (
    CloudDataTransferServiceJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID_TRANSFER = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

DAG_ID = "example_gcp_transfer"

BUCKET_NAME_SRC = f"src-bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_NAME_DST = f"dst-bucket-{DAG_ID}-{ENV_ID}".replace("_", "-")
FILE_NAME = "transfer_service_gcp_file"
FILE_URI = f"gs://{BUCKET_NAME_SRC}/{FILE_NAME}"

CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / FILE_NAME)

# [START howto_operator_gcp_transfer_create_job_body_gcp]
gcs_to_gcs_transfer_body = {
    DESCRIPTION: "description",
    STATUS: GcpTransferJobsStatus.ENABLED,
    PROJECT_ID: PROJECT_ID_TRANSFER,
    SCHEDULE: {
        SCHEDULE_START_DATE: datetime(2015, 1, 1).date(),
        SCHEDULE_END_DATE: datetime(2030, 1, 1).date(),
        START_TIME_OF_DAY: (datetime.now(tz=timezone.utc) + timedelta(seconds=120)).time(),
    },
    TRANSFER_SPEC: {
        GCS_DATA_SOURCE: {BUCKET_NAME: BUCKET_NAME_SRC},
        GCS_DATA_SINK: {BUCKET_NAME: BUCKET_NAME_DST},
        TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
    },
}
# [END howto_operator_gcp_transfer_create_job_body_gcp]

# [START howto_operator_gcp_transfer_update_job_body]
update_body = {
    PROJECT_ID: PROJECT_ID_TRANSFER,
    TRANSFER_JOB: {DESCRIPTION: "description_updated"},
    TRANSFER_JOB_FIELD_MASK: "description",
}
# [END howto_operator_gcp_transfer_update_job_body]

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "transfer", "gcp"],
) as dag:
    create_bucket_src = GCSCreateBucketOperator(
        task_id="create_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        project_id=PROJECT_ID_TRANSFER,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )

    create_bucket_dst = GCSCreateBucketOperator(
        task_id="create_bucket_dst",
        bucket_name=BUCKET_NAME_DST,
        project_id=PROJECT_ID_TRANSFER,
    )

    create_transfer = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer",
        body=gcs_to_gcs_transfer_body,
    )

    # [START howto_operator_gcp_transfer_update_job]
    update_transfer = CloudDataTransferServiceUpdateJobOperator(
        task_id="update_transfer",
        job_name="{{task_instance.xcom_pull('create_transfer')['name']}}",
        body=update_body,
    )
    # [END howto_operator_gcp_transfer_update_job]

    wait_for_transfer = CloudDataTransferServiceJobStatusSensor(
        task_id="wait_for_transfer",
        job_name="{{task_instance.xcom_pull('create_transfer')['name']}}",
        project_id=PROJECT_ID_TRANSFER,
        expected_statuses={GcpTransferOperationStatus.SUCCESS},
    )

    # [START howto_operator_gcp_transfer_run_job]
    run_transfer = CloudDataTransferServiceRunJobOperator(
        task_id="run_transfer",
        job_name="{{task_instance.xcom_pull('create_transfer')['name']}}",
        project_id=PROJECT_ID_TRANSFER,
    )
    # [END howto_operator_gcp_transfer_run_job]

    list_operations = CloudDataTransferServiceListOperationsOperator(
        task_id="list_operations",
        request_filter={
            FILTER_PROJECT_ID: PROJECT_ID_TRANSFER,
            FILTER_JOB_NAMES: ["{{task_instance.xcom_pull('create_transfer')['name']}}"],
        },
    )

    get_operation = CloudDataTransferServiceGetOperationOperator(
        task_id="get_operation",
        operation_name="{{task_instance.xcom_pull('list_operations')[0]['name']}}",
    )

    delete_transfer = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_transfer_from_gcp_job",
        job_name="{{task_instance.xcom_pull('create_transfer')['name']}}",
        project_id=PROJECT_ID_TRANSFER,
    )

    delete_bucket_dst = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME_DST, trigger_rule=TriggerRule.ALL_DONE
    )

    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src", bucket_name=BUCKET_NAME_SRC, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        [create_bucket_src, create_bucket_dst]
        >> upload_file
        >> create_transfer
        >> wait_for_transfer
        >> update_transfer
        >> run_transfer
        >> list_operations
        >> get_operation
        >> [delete_transfer, delete_bucket_src, delete_bucket_dst]
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
