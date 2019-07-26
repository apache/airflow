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
Example Airflow DAG that demonstrates interactions with Google Cloud Transfer.


This DAG relies on the following OS environment variables

* GCP_PROJECT_ID - Google Cloud Project to use for the Google Cloud Transfer Service.
* GCP_DESCRIPTION - Description of transfer job
* GCP_TRANSFER_SOURCE_AWS_BUCKET - Amazon Web Services Storage bucket from which files are copied.
  .. warning::
    You need to provide a large enough set of data so that operations do not execute too quickly.
    Otherwise, DAG will fail.
* GCP_TRANSFER_FIRST_TARGET_BUCKET - Google Cloud Storage bucket to which files are copied from AWS.
  It is also a source bucket in next step
* GCP_TRANSFER_SECOND_TARGET_BUCKET - Google Cloud Storage bucket bucket to which files are copied
* WAIT_FOR_OPERATION_POKE_INTERVAL - interval of what to check the status of the operation
  A smaller value than the default value accelerates the system test and ensures its correct execution with
  smaller quantities of files in the source bucket
  Look at documentation of :class:`~airflow.operators.sensors.BaseSensorOperator` for more information

"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import models
from airflow.contrib.hooks.gcp_transfer_hook import (
    GcpTransferOperationStatus,
    GcpTransferJobsStatus,
    TRANSFER_OPTIONS,
    PROJECT_ID,
    BUCKET_NAME,
    GCS_DATA_SINK,
    STATUS,
    DESCRIPTION,
    GCS_DATA_SOURCE,
    START_TIME_OF_DAY,
    SCHEDULE_END_DATE,
    SCHEDULE_START_DATE,
    SCHEDULE,
    AWS_S3_DATA_SOURCE,
    TRANSFER_SPEC,
    FILTER_PROJECT_ID,
    FILTER_JOB_NAMES,
    TRANSFER_JOB,
    TRANSFER_JOB_FIELD_MASK,
    ALREADY_EXISTING_IN_SINK,
)
from airflow.contrib.operators.gcp_transfer_operator import (
    GcpTransferServiceJobCreateOperator,
    GcpTransferServiceJobDeleteOperator,
    GcpTransferServiceJobUpdateOperator,
    GcpTransferServiceOperationsListOperator,
    GcpTransferServiceOperationGetOperator,
    GcpTransferServiceOperationPauseOperator,
    GcpTransferServiceOperationResumeOperator,
    GcpTransferServiceOperationCancelOperator,
)
from airflow.contrib.sensors.gcp_transfer_sensor import GCPTransferServiceWaitForJobStatusSensor
from airflow.utils.dates import days_ago

# [START howto_operator_gcp_transfer_common_variables]
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_DESCRIPTION = os.environ.get('GCP_DESCRIPTION', 'description')
GCP_TRANSFER_TARGET_BUCKET = os.environ.get('GCP_TRANSFER_TARGET_BUCKET')
WAIT_FOR_OPERATION_POKE_INTERVAL = int(os.environ.get('WAIT_FOR_OPERATION_POKE_INTERVAL', 5))

GCP_TRANSFER_SOURCE_AWS_BUCKET = os.environ.get('GCP_TRANSFER_SOURCE_AWS_BUCKET')
GCP_TRANSFER_FIRST_TARGET_BUCKET = os.environ.get(
    'GCP_TRANSFER_FIRST_TARGET_BUCKET', 'gcp-transfer-first-target'
)
GCP_TRANSFER_SECOND_TARGET_BUCKET = os.environ.get(
    'GCP_TRANSFER_SECOND_TARGET_BUCKET', 'gcp-transfer-second-target'
)
# [END howto_operator_gcp_transfer_common_variables]

# [START howto_operator_gcp_transfer_create_job_body_aws]
aws_to_gcs_transfer_body = {
    DESCRIPTION: GCP_DESCRIPTION,
    STATUS: GcpTransferJobsStatus.ENABLED,
    PROJECT_ID: GCP_PROJECT_ID,
    SCHEDULE: {
        SCHEDULE_START_DATE: datetime(2015, 1, 1).date(),
        SCHEDULE_END_DATE: datetime(2030, 1, 1).date(),
        START_TIME_OF_DAY: (datetime.utcnow() + timedelta(minutes=2)).time(),
    },
    TRANSFER_SPEC: {
        AWS_S3_DATA_SOURCE: {BUCKET_NAME: GCP_TRANSFER_SOURCE_AWS_BUCKET},
        GCS_DATA_SINK: {BUCKET_NAME: GCP_TRANSFER_FIRST_TARGET_BUCKET},
        TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
    },
}
# [END howto_operator_gcp_transfer_create_job_body_aws]

# [START howto_operator_gcp_transfer_create_job_body_gcp]
gcs_to_gcs_transfer_body = {
    DESCRIPTION: GCP_DESCRIPTION,
    STATUS: GcpTransferJobsStatus.ENABLED,
    PROJECT_ID: GCP_PROJECT_ID,
    SCHEDULE: {
        SCHEDULE_START_DATE: datetime(2015, 1, 1).date(),
        SCHEDULE_END_DATE: datetime(2030, 1, 1).date(),
        START_TIME_OF_DAY: (datetime.utcnow() + timedelta(minutes=2)).time(),
    },
    TRANSFER_SPEC: {
        GCS_DATA_SOURCE: {BUCKET_NAME: GCP_TRANSFER_FIRST_TARGET_BUCKET},
        GCS_DATA_SINK: {BUCKET_NAME: GCP_TRANSFER_SECOND_TARGET_BUCKET},
        TRANSFER_OPTIONS: {ALREADY_EXISTING_IN_SINK: True},
    },
}  # type: Dict[str, Any]
# [END howto_operator_gcp_transfer_create_job_body_gcp]

# [START howto_operator_gcp_transfer_update_job_body]
update_body = {
    PROJECT_ID: GCP_PROJECT_ID,
    TRANSFER_JOB: {DESCRIPTION: "{}_updated".format(GCP_DESCRIPTION)},
    TRANSFER_JOB_FIELD_MASK: "description",
}
# [END howto_operator_gcp_transfer_update_job_body]

list_filter_dict = {FILTER_PROJECT_ID: GCP_PROJECT_ID, FILTER_JOB_NAMES: []}

# [START howto_operator_gcp_transfer_default_args]
default_args = {'start_date': days_ago(1)}
# [END howto_operator_gcp_transfer_default_args]

with models.DAG(
    'example_gcp_transfer', default_args=default_args, schedule_interval=None  # Override to match your needs
) as dag:

    # [START howto_operator_gcp_transfer_create_job]
    create_transfer_job_from_aws = GcpTransferServiceJobCreateOperator(
        task_id="create_transfer_job_from_aws", body=aws_to_gcs_transfer_body
    )
    # [END howto_operator_gcp_transfer_create_job]

    wait_for_operation_to_start = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_operation_to_start",
        job_name="{{task_instance.xcom_pull('create_transfer_job_from_aws')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.IN_PROGRESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )

    # [START howto_operator_gcp_transfer_pause_operation]
    pause_operation = GcpTransferServiceOperationPauseOperator(
        task_id="pause_operation",
        operation_name="{{task_instance.xcom_pull('wait_for_operation_to_start', "
        "key='sensed_operations')[0]['name']}}",
    )
    # [END howto_operator_gcp_transfer_pause_operation]

    # [START howto_operator_gcp_transfer_update_job]
    update_job = GcpTransferServiceJobUpdateOperator(
        task_id="update_job",
        job_name="{{task_instance.xcom_pull('create_transfer_job_from_aws')['name']}}",
        body=update_body,
    )
    # [END howto_operator_gcp_transfer_update_job]

    # [START howto_operator_gcp_transfer_list_operations]
    list_operations = GcpTransferServiceOperationsListOperator(
        task_id="list_operations",
        request_filter={
            FILTER_PROJECT_ID: GCP_PROJECT_ID,
            FILTER_JOB_NAMES: ["{{task_instance.xcom_pull('create_transfer_job_from_aws')['name']}}"],
        },
    )
    # [END howto_operator_gcp_transfer_list_operations]

    # [START howto_operator_gcp_transfer_get_operation]
    get_operation = GcpTransferServiceOperationGetOperator(
        task_id="get_operation", operation_name="{{task_instance.xcom_pull('list_operations')[0]['name']}}"
    )
    # [END howto_operator_gcp_transfer_get_operation]

    # [START howto_operator_gcp_transfer_resume_operation]
    resume_operation = GcpTransferServiceOperationResumeOperator(
        task_id="resume_operation", operation_name="{{task_instance.xcom_pull('get_operation')['name']}}"
    )
    # [END howto_operator_gcp_transfer_resume_operation]

    # [START howto_operator_gcp_transfer_wait_operation]
    wait_for_operation_to_end = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_operation_to_end",
        job_name="{{task_instance.xcom_pull('create_transfer_job_from_aws')['name']}}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.SUCCESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )
    # [END howto_operator_gcp_transfer_wait_operation]

    job_time = datetime.utcnow() + timedelta(minutes=2)

    gcs_to_gcs_transfer_body['schedule']['startTimeOfDay'] = (datetime.utcnow() + timedelta(minutes=2)).time()

    create_transfer_job_from_gcp = GcpTransferServiceJobCreateOperator(
        task_id="create_transfer_job_from_gcp", body=gcs_to_gcs_transfer_body
    )

    wait_for_second_operation_to_start = GCPTransferServiceWaitForJobStatusSensor(
        task_id="wait_for_second_operation_to_start",
        job_name="{{ task_instance.xcom_pull('create_transfer_job_from_gcp')['name'] }}",
        project_id=GCP_PROJECT_ID,
        expected_statuses={GcpTransferOperationStatus.IN_PROGRESS},
        poke_interval=WAIT_FOR_OPERATION_POKE_INTERVAL,
    )

    # [START howto_operator_gcp_transfer_cancel_operation]
    cancel_operation = GcpTransferServiceOperationCancelOperator(
        task_id="cancel_operation",
        operation_name="{{task_instance.xcom_pull("
        "'wait_for_second_operation_to_start', key='sensed_operations')[0]['name']}}",
    )
    # [END howto_operator_gcp_transfer_cancel_operation]

    # [START howto_operator_gcp_transfer_delete_job]
    delete_transfer_from_aws_job = GcpTransferServiceJobDeleteOperator(
        task_id="delete_transfer_from_aws_job",
        job_name="{{task_instance.xcom_pull('create_transfer_job_from_aws')['name']}}",
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_gcp_transfer_delete_job]

    delete_transfer_from_gcp_job = GcpTransferServiceJobDeleteOperator(
        task_id="delete_transfer_from_gcp_job",
        job_name="{{task_instance.xcom_pull('create_transfer_job_from_gcp')['name']}}",
        project_id=GCP_PROJECT_ID,
    )

    create_transfer_job_from_aws >> wait_for_operation_to_start >> pause_operation >> \
        list_operations >> get_operation >> resume_operation >> wait_for_operation_to_end >> \
        create_transfer_job_from_gcp >> wait_for_second_operation_to_start >> cancel_operation >> \
        delete_transfer_from_aws_job >> delete_transfer_from_gcp_job
