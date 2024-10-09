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
from os import environ

import boto3

from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCreateProvisionedModelThroughputOperator,
    BedrockCustomizeModelOperator,
    BedrockInvokeModelOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.bedrock import (
    BedrockCustomizeModelCompletedSensor,
    BedrockProvisionModelThroughputCompletedSensor,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_bedrock"

# Creating a custom model takes nearly two hours. If SKIP_LONG_TASKS
# is True then these tasks will be skipped. This way we can still have
# the code snippets for docs, and we can manually run the full tests.
SKIP_LONG_TASKS = environ.get("SKIP_LONG_SYSTEM_TEST_TASKS", default=True)

# No-commitment Provisioned Throughput is currently restricted to external
# customers only and will fail with a ServiceQuotaExceededException if run
# on the AWS System Test stack.
SKIP_PROVISION_THROUGHPUT = environ.get("SKIP_RESTRICTED_SYSTEM_TEST_TASKS", default=True)


LLAMA_SHORT_MODEL_ID = "meta.llama2-13b-chat-v1"
TITAN_MODEL_ID = "amazon.titan-text-express-v1:0:8k"
TITAN_SHORT_MODEL_ID = TITAN_MODEL_ID.split(":")[0]

PROMPT = "What color is an orange?"
TRAIN_DATA = {"prompt": "what is AWS", "completion": "it's Amazon Web Services"}
HYPERPARAMETERS = {
    "epochCount": "1",
    "batchSize": "1",
    "learningRate": ".0005",
    "learningRateWarmupSteps": "0",
}


@task_group
def customize_model_workflow():
    # [START howto_operator_customize_model]
    customize_model = BedrockCustomizeModelOperator(
        task_id="customize_model",
        job_name=custom_model_job_name,
        custom_model_name=custom_model_name,
        role_arn=test_context[ROLE_ARN_KEY],
        base_model_id=f"{model_arn_prefix}{TITAN_SHORT_MODEL_ID}",
        hyperparameters=HYPERPARAMETERS,
        training_data_uri=training_data_uri,
        output_data_uri=f"s3://{bucket_name}/myOutputData",
    )
    # [END howto_operator_customize_model]

    # [START howto_sensor_customize_model]
    await_custom_model_job = BedrockCustomizeModelCompletedSensor(
        task_id="await_custom_model_job",
        job_name=custom_model_job_name,
    )
    # [END howto_sensor_customize_model]

    @task
    def delete_custom_model():
        BedrockHook().conn.delete_custom_model(modelIdentifier=custom_model_name)

    @task.branch
    def run_or_skip():
        return end_workflow.task_id if SKIP_LONG_TASKS else customize_model.task_id

    run_or_skip = run_or_skip()
    end_workflow = EmptyOperator(task_id="end_workflow", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chain(run_or_skip, Label("Long-running tasks skipped"), end_workflow)
    chain(run_or_skip, customize_model, await_custom_model_job, delete_custom_model(), end_workflow)


@task_group
def provision_throughput_workflow():
    # [START howto_operator_provision_throughput]
    provision_throughput = BedrockCreateProvisionedModelThroughputOperator(
        task_id="provision_throughput",
        model_units=1,
        provisioned_model_name=provisioned_model_name,
        model_id=f"{model_arn_prefix}{TITAN_MODEL_ID}",
    )
    # [END howto_operator_provision_throughput]

    # [START howto_sensor_provision_throughput]
    await_provision_throughput = BedrockProvisionModelThroughputCompletedSensor(
        task_id="await_provision_throughput",
        model_id=provision_throughput.output,
    )
    # [END howto_sensor_provision_throughput]

    @task
    def delete_provision_throughput(provisioned_model_id: str):
        BedrockHook().conn.delete_provisioned_model_throughput(provisionedModelId=provisioned_model_id)

    @task.branch
    def run_or_skip():
        return end_workflow.task_id if SKIP_PROVISION_THROUGHPUT else provision_throughput.task_id

    run_or_skip = run_or_skip()
    end_workflow = EmptyOperator(task_id="end_workflow", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chain(run_or_skip, Label("Quota-restricted tasks skipped"), end_workflow)
    chain(
        run_or_skip,
        provision_throughput,
        await_provision_throughput,
        delete_provision_throughput(provision_throughput.output),
        end_workflow,
    )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    bucket_name = f"{env_id}-bedrock"
    input_data_s3_key = f"{env_id}/train.jsonl"
    training_data_uri = f"s3://{bucket_name}/{input_data_s3_key}"
    custom_model_name = f"CustomModel{env_id}"
    custom_model_job_name = f"CustomizeModelJob{env_id}"
    provisioned_model_name = f"ProvisionedModel{env_id}"
    model_arn_prefix = f"arn:aws:bedrock:{boto3.session.Session().region_name}::foundation-model/"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_training_data = S3CreateObjectOperator(
        task_id="upload_data",
        s3_bucket=bucket_name,
        s3_key=input_data_s3_key,
        data=json.dumps(TRAIN_DATA),
    )

    # [START howto_operator_invoke_llama_model]
    invoke_llama_model = BedrockInvokeModelOperator(
        task_id="invoke_llama",
        model_id=LLAMA_SHORT_MODEL_ID,
        input_data={"prompt": PROMPT},
    )
    # [END howto_operator_invoke_llama_model]

    # [START howto_operator_invoke_titan_model]
    invoke_titan_model = BedrockInvokeModelOperator(
        task_id="invoke_titan",
        model_id=TITAN_SHORT_MODEL_ID,
        input_data={"inputText": PROMPT},
    )
    # [END howto_operator_invoke_titan_model]

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
        upload_training_data,
        # TEST BODY
        [invoke_llama_model, invoke_titan_model],
        customize_model_workflow(),
        provision_throughput_workflow(),
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
