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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessStopApplicationOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessApplicationSensor, EmrServerlessJobSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_emr_serverless"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"


sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    bucket_name = f"{env_id}-emr-serverless-bucket"
    region = boto3.session.Session().region_name
    entryPoint = f"s3://{region}.elasticmapreduce/emr-containers/samples/wordcount/scripts/wordcount.py"
    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=bucket_name)

    SPARK_JOB_DRIVER = {
        "sparkSubmit": {
            "entryPoint": entryPoint,
            "entryPointArguments": [f"s3://{bucket_name}/output"],
            "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g\
                --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
        }
    }

    SPARK_CONFIGURATION_OVERRIDES = {
        "monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": f"s3://{bucket_name}/logs"}}
    }

    # [START howto_operator_emr_serverless_create_application]
    emr_serverless_app = EmrServerlessCreateApplicationOperator(
        task_id="create_emr_serverless_task",
        release_label="emr-6.6.0",
        job_type="SPARK",
        config={"name": "new_application"},
    )
    # [END howto_operator_emr_serverless_create_application]

    # EmrServerlessCreateApplicationOperator waits by default, setting as False to test the Sensor below.
    emr_serverless_app.wait_for_completion = False

    emr_serverless_app_id = emr_serverless_app.output

    # [START howto_sensor_emr_serverless_application]
    wait_for_app_creation = EmrServerlessApplicationSensor(
        task_id="wait_for_app_creation",
        application_id=emr_serverless_app_id,
    )
    # [END howto_sensor_emr_serverless_application]
    wait_for_app_creation.poke_interval = 1

    # [START howto_operator_emr_serverless_start_job]
    start_job = EmrServerlessStartJobOperator(
        task_id="start_emr_serverless_job",
        application_id=emr_serverless_app_id,
        execution_role_arn=role_arn,
        job_driver=SPARK_JOB_DRIVER,
        configuration_overrides=SPARK_CONFIGURATION_OVERRIDES,
    )
    # [END howto_operator_emr_serverless_start_job]
    start_job.wait_for_completion = False

    # [START howto_sensor_emr_serverless_job]
    wait_for_job = EmrServerlessJobSensor(
        task_id="wait_for_job",
        application_id=emr_serverless_app_id,
        job_run_id=start_job.output,
        # the default is to wait for job completion, here we just wait for the job to be running.
        target_states={*EmrServerlessHook.JOB_SUCCESS_STATES, "RUNNING"},
    )
    # [END howto_sensor_emr_serverless_job]
    wait_for_job.poke_interval = 10

    # [START howto_operator_emr_serverless_stop_application]
    stop_app = EmrServerlessStopApplicationOperator(
        task_id="stop_application",
        application_id=emr_serverless_app_id,
        force_stop=True,
    )
    # [END howto_operator_emr_serverless_stop_application]
    stop_app.waiter_check_interval_seconds = 1

    # [START howto_operator_emr_serverless_delete_application]
    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_application",
        application_id=emr_serverless_app_id,
    )
    # [END howto_operator_emr_serverless_delete_application]
    delete_app.waiter_check_interval_seconds = 1
    delete_app.trigger_rule = TriggerRule.ALL_DONE

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        # TEST BODY
        emr_serverless_app,
        wait_for_app_creation,
        start_job,
        wait_for_job,
        stop_app,
        # TEST TEARDOWN
        delete_app,
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
