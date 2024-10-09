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

import contextlib
import json
from datetime import datetime

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

"""
Prerequisites:
1. The account which runs this test must manually be activated in Quicksight here:
https://quicksight.aws.amazon.com/sn/console/signup?#
2. The activation process creates an IAM Role called `aws-quicksight-service-role-v0`.
 You have to add a policy named 'AWSQuickSightS3Policy' with the S3 access permissions.
 The policy name is enforced, and the permissions json can be copied from `AmazonS3FullAccess`.

NOTES:  If Create Ingestion fails for any reason, that ingestion name will remain in use and
 future runs will stall with the sensor returning a status of QUEUED "forever".  If you run
 into this behavior, changing the template for the ingestion name or the ENV_ID and re-running
 the test should resolve the issue.
"""

DAG_ID = "example_quicksight"

sys_test_context_task = SystemTestContextBuilder().build()

SAMPLE_DATA_COLUMNS = ["Project", "Year"]
SAMPLE_DATA = """'Airflow','2015'
    'OpenOffice','2012'
    'Subversion','2000'
    'NiFi','2006'
"""


@task
def get_aws_account_id() -> int:
    return boto3.client("sts").get_caller_identity()["Account"]


@task
def create_quicksight_data_source(
    aws_account_id: str, datasource_name: str, bucket: str, manifest_key: str
) -> str:
    response = boto3.client("quicksight").create_data_source(
        AwsAccountId=aws_account_id,
        DataSourceId=datasource_name,
        Name=datasource_name,
        Type="S3",
        DataSourceParameters={
            "S3Parameters": {"ManifestFileLocation": {"Bucket": bucket, "Key": manifest_key}}
        },
    )
    return response["Arn"]


@task
def create_quicksight_dataset(aws_account_id: int, dataset_name: str, data_source_arn: str) -> None:
    table_map = {
        "default": {
            "S3Source": {
                "DataSourceArn": data_source_arn,
                "InputColumns": [{"Name": name, "Type": "STRING"} for name in SAMPLE_DATA_COLUMNS],
            }
        }
    }

    boto3.client("quicksight").create_data_set(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_name,
        Name=dataset_name,
        PhysicalTableMap=table_map,
        ImportMode="SPICE",
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_quicksight_data_source(aws_account_id: str, datasource_name: str):
    boto3.client("quicksight").delete_data_source(AwsAccountId=aws_account_id, DataSourceId=datasource_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dataset(aws_account_id: str, dataset_name: str):
    boto3.client("quicksight").delete_data_set(AwsAccountId=aws_account_id, DataSetId=dataset_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_ingestion(aws_account_id: str, dataset_name: str, ingestion_name: str) -> None:
    client = boto3.client("quicksight")
    with contextlib.suppress(client.exceptions.ResourceNotFoundException):
        # suppress ResourceNotFoundException: Ingestion has already terminated on its own.
        client.cancel_ingestion(
            AwsAccountId=aws_account_id,
            DataSetId=dataset_name,
            IngestionId=ingestion_name,
        )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    account_id = get_aws_account_id()

    env_id = test_context[ENV_ID_KEY]
    bucket_name = f"{env_id}-quicksight-bucket"
    data_filename = "sample_data.csv"
    dataset_id = f"{env_id}-data-set"
    datasource_id = f"{env_id}-data-source"
    ingestion_id = f"{env_id}-ingestion"
    manifest_filename = f"{env_id}-manifest.json"
    manifest_contents = {"fileLocations": [{"URIs": [f"s3://{bucket_name}/{data_filename}"]}]}

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=bucket_name)

    upload_manifest_file = S3CreateObjectOperator(
        task_id="upload_manifest_file",
        s3_bucket=bucket_name,
        s3_key=manifest_filename,
        data=json.dumps(manifest_contents),
        replace=True,
    )

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=bucket_name,
        s3_key=data_filename,
        data=SAMPLE_DATA,
        replace=True,
    )

    data_source = create_quicksight_data_source(
        aws_account_id=account_id,
        datasource_name=datasource_id,
        bucket=bucket_name,
        manifest_key=manifest_filename,
    )

    create_dataset = create_quicksight_dataset(account_id, dataset_id, data_source)

    # [START howto_operator_quicksight_create_ingestion]
    create_ingestion = QuickSightCreateIngestionOperator(
        task_id="create_ingestion",
        data_set_id=dataset_id,
        ingestion_id=ingestion_id,
    )
    # [END howto_operator_quicksight_create_ingestion]

    # QuickSightCreateIngestionOperator waits by default, setting as False to test the Sensor below.
    create_ingestion.wait_for_completion = False

    # If this sensor appears to freeze with a "QUEUED" status, see note above.
    # [START howto_sensor_quicksight]
    await_job = QuickSightSensor(
        task_id="await_job",
        data_set_id=dataset_id,
        ingestion_id=ingestion_id,
    )
    # [END howto_sensor_quicksight]
    await_job.poke_interval = 10

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        account_id,
        create_s3_bucket,
        upload_manifest_file,
        upload_sample_data,
        data_source,
        create_dataset,
        # TEST BODY
        create_ingestion,
        await_job,
        # TEST TEARDOWN
        delete_dataset(account_id, dataset_id),
        delete_quicksight_data_source(account_id, datasource_id),
        delete_ingestion(account_id, dataset_id, ingestion_id),
        delete_bucket,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
