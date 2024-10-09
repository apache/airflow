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
from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task_group
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.comprehend import ComprehendStartPiiEntitiesDetectionJobOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.comprehend import (
    ComprehendStartPiiEntitiesDetectionJobCompletedSensor,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_comprehend"
INPUT_S3_KEY_START_PII_ENTITIES_DETECTION_JOB = "start-pii-entities-detection-job/sample_data.txt"

SAMPLE_DATA = {
    "username": "bob1234",
    "name": "Bob",
    "sex": "M",
    "address": "1773 Raymond Ville Suite 682",
    "mail": "test@hotmail.com",
}


@task_group
def pii_entities_detection_job_workflow():
    # [START howto_operator_start_pii_entities_detection_job]
    start_pii_entities_detection_job = ComprehendStartPiiEntitiesDetectionJobOperator(
        task_id="start_pii_entities_detection_job",
        input_data_config=input_data_configurations,
        output_data_config=output_data_configurations,
        mode="ONLY_REDACTION",
        data_access_role_arn=test_context[ROLE_ARN_KEY],
        language_code="en",
        start_pii_entities_kwargs=pii_entities_kwargs,
    )
    # [END howto_operator_start_pii_entities_detection_job]
    start_pii_entities_detection_job.wait_for_completion = False

    # [START howto_sensor_start_pii_entities_detection_job]
    await_start_pii_entities_detection_job = ComprehendStartPiiEntitiesDetectionJobCompletedSensor(
        task_id="await_start_pii_entities_detection_job", job_id=start_pii_entities_detection_job.output
    )
    # [END howto_sensor_start_pii_entities_detection_job]

    chain(start_pii_entities_detection_job, await_start_pii_entities_detection_job)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    bucket_name = f"{env_id}-comprehend"
    input_data_configurations = {
        "S3Uri": f"s3://{bucket_name}/{INPUT_S3_KEY_START_PII_ENTITIES_DETECTION_JOB}",
        "InputFormat": "ONE_DOC_PER_LINE",
    }
    output_data_configurations = {"S3Uri": f"s3://{bucket_name}/redacted_output/"}
    pii_entities_kwargs = {
        "RedactionConfig": {
            "PiiEntityTypes": ["NAME", "ADDRESS"],
            "MaskMode": "REPLACE_WITH_PII_ENTITY_TYPE",
        }
    }

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=bucket_name,
        s3_key=INPUT_S3_KEY_START_PII_ENTITIES_DETECTION_JOB,
        data=json.dumps(SAMPLE_DATA),
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        upload_sample_data,
        # TEST BODY
        pii_entities_detection_job_workflow(),
        # TEST TEARDOWN
        delete_bucket,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
