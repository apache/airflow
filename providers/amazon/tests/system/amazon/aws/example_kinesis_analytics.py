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

import datetime as dt
import json
import random
from datetime import datetime

import boto3

from airflow import settings
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.operators.kinesis_analytics import (
    KinesisAnalyticsV2CreateApplicationOperator,
    KinesisAnalyticsV2StartApplicationOperator,
    KinesisAnalyticsV2StopApplicationOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.kinesis_analytics import (
    KinesisAnalyticsV2StartApplicationCompletedSensor,
    KinesisAnalyticsV2StopApplicationCompletedSensor,
)
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task, task_group
else:
    # Airflow 2 path
    from airflow.decorators import task, task_group  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_kinesis_analytics"


@task_group
def kinesis_analytics_v2_workflow():
    # [START howto_operator_create_application]
    create_application = KinesisAnalyticsV2CreateApplicationOperator(
        task_id="create_application",
        application_name=application_name,
        runtime_environment="FLINK-1_18",
        service_execution_role=test_context[ROLE_ARN_KEY],
        create_application_kwargs={
            "ApplicationConfiguration": {
                "FlinkApplicationConfiguration": {
                    "ParallelismConfiguration": {
                        "ConfigurationType": "CUSTOM",
                        "Parallelism": 2,
                        "ParallelismPerKPU": 1,
                        "AutoScalingEnabled": False,
                    }
                },
                "EnvironmentProperties": {
                    "PropertyGroups": [
                        {
                            "PropertyGroupId": "BlueprintMetadata",
                            "PropertyMap": {
                                "AWSRegion": region_name,
                                "BlueprintName": "KDS_FLINK-DATASTREAM-JAVA_S3",
                                "BucketName": f"s3://{bucket_name}/",
                                "PartitionFormat": "yyyy-MM-dd-HH",
                                "StreamInitialPosition": "TRIM_HORIZON",
                                "StreamName": stream_name,
                            },
                        },
                    ]
                },
                "ApplicationCodeConfiguration": {
                    "CodeContent": {
                        "S3ContentLocation": {
                            "BucketARN": f"arn:aws:s3:::{bucket_name}",
                            "FileKey": "code/kds-to-s3-datastream-java-1.0.1.jar",
                        },
                    },
                    "CodeContentType": "ZIPFILE",
                },
            }
        },
    )
    # [END howto_operator_create_application]

    # [START howto_operator_start_application]
    start_application = KinesisAnalyticsV2StartApplicationOperator(
        task_id="start_application",
        application_name=application_name,
    )
    # [END howto_operator_start_application]
    start_application.wait_for_completion = False

    # [START howto_sensor_start_application]
    await_start_application = KinesisAnalyticsV2StartApplicationCompletedSensor(
        task_id="await_start_application",
        application_name=application_name,
    )
    # [END howto_sensor_start_application]

    # [START howto_operator_stop_application]
    stop_application = KinesisAnalyticsV2StopApplicationOperator(
        task_id="stop_application",
        application_name=application_name,
    )
    # [END howto_operator_stop_application]
    stop_application.wait_for_completion = False
    # With the default `force=False` the test fails  because the data retention policy is blocking the deletion.
    stop_application.force = True

    # [START howto_sensor_stop_application]
    await_stop_application = KinesisAnalyticsV2StopApplicationCompletedSensor(
        task_id="await_stop_application",
        application_name=application_name,
    )
    # [END howto_sensor_stop_application]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_application(app_name: str):
        kinesis_analytics_v2_hook = KinesisAnalyticsV2Hook()
        response = kinesis_analytics_v2_hook.conn.describe_application(ApplicationName=app_name)
        kinesis_analytics_v2_hook.conn.delete_application(
            ApplicationName=app_name, CreateTimestamp=response["ApplicationDetail"]["CreateTimestamp"]
        )

    chain(
        create_application,
        start_application,
        await_start_application,
        stop_application,
        await_stop_application,
        delete_application(application_name),
    )


@task_group
def copy_jar_to_s3(bucket: str):
    """

    Copy application code to S3 using HttpToS3Operator.

    :param bucket: Name of the Amazon S3 bucket.
    """

    @task
    def create_connection(conn_id):
        conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host="https://github.com/",
        )
        if settings.Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = settings.Session()
        session.add(conn)
        session.commit()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_connection(conn_id: str):
        if settings.Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = settings.Session()
        conn_to_details = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        session.delete(conn_to_details)
        session.commit()

    copy_jar_file = HttpToS3Operator(
        task_id="copy_jar_file",
        http_conn_id=http_conn_id,
        endpoint="awslabs/managed-service-for-apache-flink-blueprints/releases/download/v2.0.1/kds-to-s3-datastream-java-1.0.1.jar",
        s3_bucket=bucket,
        s3_key="code/kds-to-s3-datastream-java-1.0.1.jar",
    )

    chain(create_connection(http_conn_id), copy_jar_file, delete_connection(http_conn_id))


@task
def create_kinesis_stream(stream: str, region: str):
    """
    Create kinesis stream and put some sample data.

    :param stream: Name of the kinesis stream.
    :param region: Region name
    """
    client = boto3.client("kinesis", region_name=region)
    client.create_stream(StreamName=stream, ShardCount=1, StreamModeDetails={"StreamMode": "PROVISIONED"})
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=stream, WaiterConfig={"Delay": 60, "MaxAttempts": 4})

    def get_data():
        return {
            "event_time": dt.datetime.now().isoformat(),
            "ticker": random.choice(["AAPL", "AMZN", "MSFT", "INTC", "TBV"]),
            "price": round(random.random() * 100, 2),
        }

    for _ in range(2):
        data = get_data()
        client.put_record(
            StreamARN=f"arn:aws:kinesis:{region}:{account_id}:stream/{stream}",
            Data=json.dumps(data),
            PartitionKey=data["ticker"],
        )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_kinesis_stream(stream: str, region: str):
    client = boto3.client("kinesis", region_name=region)
    client.delete_stream(StreamName=stream, EnforceConsumerDeletion=True)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    bucket_name = f"{env_id}-kinesis-analytics"
    application_name = f"{env_id}-test-app"
    http_conn_id = f"{env_id}-git"
    region_name = boto3.session.Session().region_name
    stream_name = f"{env_id}-test-stream"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
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
        create_kinesis_stream(stream=stream_name, region=region_name),
        copy_jar_to_s3(bucket=bucket_name),
        # TEST BODY
        kinesis_analytics_v2_workflow(),
        # TEST TEARDOWN
        delete_kinesis_stream(stream=stream_name, region=region_name),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
