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

import boto3

from airflow.providers.amazon.aws.operators.appflow import (
    AppflowRunOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_appflow_run"


@task
def create_s3_to_s3_flow(flow_name: str, bucket_name: str, source_folder: str):
    """creates a flow that takes a CSV and converts it to a json containing the same data"""
    client = boto3.client("appflow")
    client.create_flow(
        flowName=flow_name,
        triggerConfig={"triggerType": "OnDemand"},
        sourceFlowConfig={
            "connectorType": "S3",
            "sourceConnectorProperties": {
                "S3": {
                    "bucketName": bucket_name,
                    "bucketPrefix": source_folder,
                    "s3InputFormatConfig": {"s3InputFileType": "CSV"},
                },
            },
        },
        destinationFlowConfigList=[
            {
                "connectorType": "S3",
                "destinationConnectorProperties": {
                    "S3": {
                        "bucketName": bucket_name,
                        "s3OutputFormatConfig": {
                            "fileType": "JSON",
                            "aggregationConfig": {
                                "aggregationType": "None",
                            },
                        },
                    }
                },
            },
        ],
        tasks=[
            {
                "sourceFields": ["col1", "col2"],
                "connectorOperator": {"S3": "PROJECTION"},
                "taskType": "Filter",
            },
            {
                "sourceFields": ["col1"],
                "connectorOperator": {"S3": "NO_OP"},
                "destinationField": "col1",
                "taskType": "Map",
                "taskProperties": {"DESTINATION_DATA_TYPE": "string", "SOURCE_DATA_TYPE": "string"},
            },
            {
                "sourceFields": ["col2"],
                "connectorOperator": {"S3": "NO_OP"},
                "destinationField": "col2",
                "taskType": "Map",
                "taskProperties": {"DESTINATION_DATA_TYPE": "string", "SOURCE_DATA_TYPE": "string"},
            },
        ],
    )


@task
def setup_bucket_permissions(bucket_name):
    s3 = boto3.client("s3")
    s3.put_bucket_policy(
        Bucket=bucket_name,
        Policy=json.dumps(
            {
                "Version": "2008-10-17",
                "Statement": [
                    {
                        "Sid": "AllowAppFlowSourceActions",
                        "Effect": "Allow",
                        "Principal": {"Service": "appflow.amazonaws.com"},
                        "Action": ["s3:ListBucket", "s3:GetObject"],
                        "Resource": [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"],
                    },
                    {
                        "Sid": "AllowAppFlowDestinationActions",
                        "Effect": "Allow",
                        "Principal": {"Service": "appflow.amazonaws.com"},
                        "Action": [
                            "s3:PutObject",
                            "s3:AbortMultipartUpload",
                            "s3:ListMultipartUploadParts",
                            "s3:ListBucketMultipartUploads",
                            "s3:GetBucketAcl",
                            "s3:PutObjectAcl",
                        ],
                        "Resource": [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"],
                    },
                ],
            }
        ),
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_flow(flow_name: str):
    client = boto3.client("appflow")
    client.delete_flow(flowName=flow_name, forceDelete=True)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    flow_name = f"{env_id}-flow"
    bucket_name = f"{env_id}-for-appflow"
    source_folder = "source"

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    upload_csv = S3CreateObjectOperator(
        task_id="upload_csv",
        s3_bucket=bucket_name,
        s3_key="source_folder/data.csv",
        data="""col1,col2\n"data1","data2"\n""",
        replace=True,
    )

    # [START howto_operator_appflow_run]
    run_flow = AppflowRunOperator(
        task_id="run_flow",
        flow_name=flow_name,
    )
    # [END howto_operator_appflow_run]
    run_flow.poll_interval = 1

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
        setup_bucket_permissions(bucket_name),
        upload_csv,
        create_s3_to_s3_flow(flow_name, bucket_name, source_folder),
        # TEST BODY
        run_flow,
        # TEARDOWN
        delete_flow(flow_name),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
