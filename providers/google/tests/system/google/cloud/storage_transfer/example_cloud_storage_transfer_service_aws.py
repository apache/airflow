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
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    ALREADY_EXISTING_IN_SINK,
    AWS_S3_DATA_SOURCE,
    BUCKET_NAME,
    DESCRIPTION,
    FILTER_JOB_NAMES,
    FILTER_PROJECT_ID,
    GCS_DATA_SINK,
    JOB_NAME,
    PROJECT_ID,
    SCHEDULE,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    START_TIME_OF_DAY,
    STATUS,
    TRANSFER_OPTIONS,
    TRANSFER_SPEC,
    GcpTransferJobsStatus,
    GcpTransferOperationStatus,
)
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCancelOperationOperator,
    CloudDataTransferServiceCreateJobOperator,
    CloudDataTransferServiceDeleteJobOperator,
    CloudDataTransferServiceGetOperationOperator,
    CloudDataTransferServiceListOperationsOperator,
    CloudDataTransferServicePauseOperationOperator,
    CloudDataTransferServiceResumeOperationOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import (
    CloudDataTransferServiceJobStatusSensor,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "example_gcp_transfer_aws"

EXAMPLE_BUCKET = "airflow-system-tests-resources"
EXAMPLE_FILE = "storage-transfer/big_file.dat"
BUCKET_SOURCE_AWS = f"bucket-aws-{DAG_ID}-{ENV_ID}".replace("_", "-")
BUCKET_TARGET_GCS = f"bucket-gcs-{DAG_ID}-{ENV_ID}".replace("_", "-")
WAIT_FOR_OPERATION_POKE_INTERVAL = int(os.environ.get("WAIT_FOR_OPERATION_POKE_INTERVAL", str(5)))

GCP_DESCRIPTION = "description"
GCP_TRANSFER_JOB_NAME = f"transferJobs/sampleJob-{DAG_ID}-{ENV_ID}".replace("-", "_")
GCP_TRANSFER_JOB_2_NAME = f"transferJobs/sampleJob2-{DAG_ID}-{ENV_ID}".replace("-", "_")

# [START howto_operator_gcp_transfer_create_job_body_aws]
aws_to_gcs_transfer_body = {
    DESCRIPTION: GCP_DESCRIPTION,
    STATUS: GcpTransferJobsStatus.ENABLED,
    PROJECT_ID: GCP_PROJECT_ID,
    JOB_NAME: GCP_TRANSFER_JOB_NAME,
    SCHEDULE: {
        SCHEDULE_START_DATE: datetime(2015, 1, 1).date(),
        SCHEDULE_END_DATE: datetime(2030, 1, 1).date(),
        START_TIME_OF_DAY: (datetime.now(tz=timezone.utc) + timedelta(minutes=1)).time(),
    },
    TRANSFER_SPEC: {
        AWS_S3_DATA_SOURCE: {BUCKET_NAME: BUCKET_SOURCE_AWS},
        GCS_DATA_SINK: {BUCKET_NAME: BUCKET_TARGET_GCS},
        TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
    },
}
# [END howto_operator_gcp_transfer_create_job_body_aws]

aws_to_gcs_transfer_body_2 = deepcopy(aws_to_gcs_transfer_body)
aws_to_gcs_transfer_body_2[JOB_NAME] = GCP_TRANSFER_JOB_2_NAME

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "aws", "gcs", "transfer"],
) as dag:
    create_bucket_s3 = S3CreateBucketOperator(
        task_id="create_bucket_s3", bucket_name=BUCKET_SOURCE_AWS, region_name="us-east-1"
    )

    upload_file_to_s3 = GCSToS3Operator(
        task_id="upload_file_to_s3",
        gcp_user_project=GCP_PROJECT_ID,
        gcs_bucket=EXAMPLE_BUCKET,
        prefix=EXAMPLE_FILE,
        dest_s3_key=f"s3://{BUCKET_SOURCE_AWS}",
        replace=True,
    )
    #
    create_bucket_gcs = GCSCreateBucketOperator(
        task_id="create_bucket_gcs",
        bucket_name=BUCKET_TARGET_GCS,
        project_id=GCP_PROJECT_ID,
    )

    # [START howto_operator_gcp_transfer_create_job]
    create_transfer_job_s3_to_gcs = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer_job_s3_to_gcs", body=aws_to_gcs_transfer_body
    )
    # [END howto_operator_gcp_transfer_create_job]

    wait_for_operation_to_start = CloudDataTransferServiceJobStatusSensor(
        task_id="wait_for_operation_to_start",
        job_name="{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.IN_PROGRESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )

    # [START howto_operator_gcp_transfer_pause_operation]
    pause_operation = CloudDataTransferServicePauseOperationOperator(
        task_id="pause_operation",
        operation_name="{{task_instance.xcom_pull('wait_for_operation_to_start', "
        "key='sensed_operations')[0]['name']}}",
    )
    # [END howto_operator_gcp_transfer_pause_operation]

    # [START howto_operator_gcp_transfer_list_operations]
    list_operations = CloudDataTransferServiceListOperationsOperator(
        task_id="list_operations",
        request_filter={
            FILTER_PROJECT_ID: GCP_PROJECT_ID,
            FILTER_JOB_NAMES: ["{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs')['name']}}"],
        },
    )
    # [END howto_operator_gcp_transfer_list_operations]

    # [START howto_operator_gcp_transfer_get_operation]
    get_operation = CloudDataTransferServiceGetOperationOperator(
        task_id="get_operation", operation_name="{{task_instance.xcom_pull('list_operations')[0]['name']}}"
    )
    # [END howto_operator_gcp_transfer_get_operation]

    # [START howto_operator_gcp_transfer_resume_operation]
    resume_operation = CloudDataTransferServiceResumeOperationOperator(
        task_id="resume_operation", operation_name="{{task_instance.xcom_pull('get_operation')['name']}}"
    )
    # [END howto_operator_gcp_transfer_resume_operation]

    # [START howto_operator_gcp_transfer_wait_operation]
    wait_for_operation_to_end = CloudDataTransferServiceJobStatusSensor(
        task_id="wait_for_operation_to_end",
        job_name="{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.SUCCESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )
    # [END howto_operator_gcp_transfer_wait_operation]

    create_second_transfer_job_from_aws = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer_job_s3_to_gcs_2", body=aws_to_gcs_transfer_body_2
    )

    wait_for_operation_to_start_2 = CloudDataTransferServiceJobStatusSensor(
        task_id="wait_for_operation_to_start_2",
        job_name="{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs_2')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.IN_PROGRESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )

    # [START howto_operator_gcp_transfer_cancel_operation]
    cancel_operation = CloudDataTransferServiceCancelOperationOperator(
        task_id="cancel_operation",
        operation_name="{{task_instance.xcom_pull("
        "'wait_for_operation_to_start_2', key='sensed_operations')[0]['name']}}",
    )
    # [END howto_operator_gcp_transfer_cancel_operation]

    # [START howto_operator_gcp_transfer_delete_job]
    delete_transfer_job_s3_to_gcs = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_transfer_job_s3_to_gcs",
        job_name="{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs')['name']}}",
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_gcp_transfer_delete_job]

    delete_transfer_job_s3_to_gcs_2 = CloudDataTransferServiceDeleteJobOperator(
        task_id="delete_transfer_job_s3_to_gcs_2",
        job_name="{{task_instance.xcom_pull('create_transfer_job_s3_to_gcs_2')['name']}}",
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket_s3 = S3DeleteBucketOperator(
        task_id="delete_bucket_s3",
        bucket_name=BUCKET_SOURCE_AWS,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket_gcs = GCSDeleteBucketOperator(
        task_id="delete_bucket_gcs",
        bucket_name=BUCKET_TARGET_GCS,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        [create_bucket_s3 >> upload_file_to_s3, create_bucket_gcs]
        # TEST BODY
        >> create_transfer_job_s3_to_gcs
        >> wait_for_operation_to_start
        >> pause_operation
        >> list_operations
        >> get_operation
        >> resume_operation
        >> wait_for_operation_to_end
        >> create_second_transfer_job_from_aws
        >> wait_for_operation_to_start_2
        >> cancel_operation
        # TEST TEARDOWN
        >> [
            delete_transfer_job_s3_to_gcs,
            delete_transfer_job_s3_to_gcs_2,
            delete_bucket_gcs,
            delete_bucket_s3,
        ]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
