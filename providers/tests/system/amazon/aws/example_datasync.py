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

from datetime import datetime

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.datasync import DataSyncOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_datasync"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()


def get_s3_bucket_arn(bucket_name):
    return f"arn:aws:s3:::{bucket_name}"


def create_location(bucket_name, role_arn):
    client = boto3.client("datasync")
    response = client.create_location_s3(
        Subdirectory="test",
        S3BucketArn=get_s3_bucket_arn(bucket_name),
        S3Config={
            "BucketAccessRoleArn": role_arn,
        },
    )
    return response["LocationArn"]


@task
def create_source_location(bucket_source, role_arn):
    return create_location(bucket_source, role_arn)


@task
def create_destination_location(bucket_destination, role_arn):
    return create_location(bucket_destination, role_arn)


@task
def create_task(**kwargs):
    client = boto3.client("datasync")
    response = client.create_task(
        SourceLocationArn=kwargs["ti"].xcom_pull("create_source_location"),
        DestinationLocationArn=kwargs["ti"].xcom_pull("create_destination_location"),
    )
    return response["TaskArn"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_task(task_arn):
    client = boto3.client("datasync")
    client.delete_task(
        TaskArn=task_arn,
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_task_created_by_operator(**kwargs):
    client = boto3.client("datasync")
    client.delete_task(
        TaskArn=kwargs["ti"].xcom_pull("create_and_execute_task")["TaskArn"],
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def list_locations(bucket_source, bucket_destination):
    client = boto3.client("datasync")
    return client.list_locations(
        Filters=[
            {
                "Name": "LocationUri",
                "Values": [
                    f"s3://{bucket_source}/test/",
                    f"s3://{bucket_destination}/test/",
                    f"s3://{bucket_source}/test_create/",
                    f"s3://{bucket_destination}/test_create/",
                ],
                "Operator": "In",
            }
        ]
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_locations(locations):
    client = boto3.client("datasync")
    for location in locations["Locations"]:
        client.delete_location(
            LocationArn=location["LocationArn"],
        )


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()

    s3_bucket_source: str = f"{test_context[ENV_ID_KEY]}-datasync-bucket-source"
    s3_bucket_destination: str = f"{test_context[ENV_ID_KEY]}-datasync-bucket-destination"

    create_s3_bucket_source = S3CreateBucketOperator(
        task_id="create_s3_bucket_source", bucket_name=s3_bucket_source
    )

    create_s3_bucket_destination = S3CreateBucketOperator(
        task_id="create_s3_bucket_destination", bucket_name=s3_bucket_destination
    )

    source_location = create_source_location(s3_bucket_source, test_context[ROLE_ARN_KEY])
    destination_location = create_destination_location(s3_bucket_destination, test_context[ROLE_ARN_KEY])

    created_task_arn = create_task()

    # [START howto_operator_datasync_specific_task]
    # Execute a specific task
    execute_task_by_arn = DataSyncOperator(
        task_id="execute_task_by_arn",
        task_arn=created_task_arn,
    )
    # [END howto_operator_datasync_specific_task]

    # DataSyncOperator waits by default, setting as False to test the Sensor below.
    execute_task_by_arn.wait_for_completion = False

    # [START howto_operator_datasync_search_task]
    # Search and execute a task
    execute_task_by_locations = DataSyncOperator(
        task_id="execute_task_by_locations",
        source_location_uri=f"s3://{s3_bucket_source}/test",
        destination_location_uri=f"s3://{s3_bucket_destination}/test",
        # Only transfer files from /test/subdir folder
        task_execution_kwargs={
            "Includes": [{"FilterType": "SIMPLE_PATTERN", "Value": "/test/subdir"}],
        },
    )
    # [END howto_operator_datasync_search_task]

    # DataSyncOperator waits by default, setting as False to test the Sensor below.
    execute_task_by_locations.wait_for_completion = False

    # [START howto_operator_datasync_create_task]
    # Create a task (the task does not exist)
    create_and_execute_task = DataSyncOperator(
        task_id="create_and_execute_task",
        source_location_uri=f"s3://{s3_bucket_source}/test_create",
        destination_location_uri=f"s3://{s3_bucket_destination}/test_create",
        create_task_kwargs={"Name": "Created by Airflow"},
        create_source_location_kwargs={
            "Subdirectory": "test_create",
            "S3BucketArn": get_s3_bucket_arn(s3_bucket_source),
            "S3Config": {
                "BucketAccessRoleArn": test_context[ROLE_ARN_KEY],
            },
        },
        create_destination_location_kwargs={
            "Subdirectory": "test_create",
            "S3BucketArn": get_s3_bucket_arn(s3_bucket_destination),
            "S3Config": {
                "BucketAccessRoleArn": test_context[ROLE_ARN_KEY],
            },
        },
        delete_task_after_execution=False,
    )
    # [END howto_operator_datasync_create_task]

    # DataSyncOperator waits by default, setting as False to test the Sensor below.
    create_and_execute_task.wait_for_completion = False

    locations_task = list_locations(s3_bucket_source, s3_bucket_destination)
    delete_locations_task = delete_locations(locations_task)

    delete_s3_bucket_source = S3DeleteBucketOperator(
        task_id="delete_s3_bucket_source",
        bucket_name=s3_bucket_source,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_s3_bucket_destination = S3DeleteBucketOperator(
        task_id="delete_s3_bucket_destination",
        bucket_name=s3_bucket_destination,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket_source,
        create_s3_bucket_destination,
        source_location,
        destination_location,
        created_task_arn,
        # TEST BODY
        execute_task_by_arn,
        execute_task_by_locations,
        create_and_execute_task,
        # TEST TEARDOWN
        delete_task(created_task_arn),
        delete_task_created_by_operator(),
        locations_task,
        delete_locations_task,
        delete_s3_bucket_source,
        delete_s3_bucket_destination,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
