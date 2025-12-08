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
import logging
from datetime import datetime
from tempfile import NamedTemporaryFile

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockBatchInferenceOperator,
    BedrockInvokeModelOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.bedrock import BedrockBatchInferenceSensor

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

log = logging.getLogger(__name__)

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

DAG_ID = "example_bedrock_batch_inference"

#######################################################################
# NOTE:
#   Access to the following foundation model must be requested via
#   the Amazon Bedrock console and may take up to 24 hours to apply:
#######################################################################

CLAUDE_MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
ANTHROPIC_VERSION = "bedrock-2023-05-31"

# Batch inferences currently require a minimum of 100 prompts per batch.
MIN_NUM_PROMPTS = 100
PROMPT_TEMPLATE = "Even numbers are red. Odd numbers are blue. What color is {n}?"


@task
def generate_prompts(_env_id: str, _bucket: str, _key: str):
    """
    Bedrock Batch Inference requires one or more jsonl-formatted files in an S3 bucket.

    The JSONL format requires one serialized json object per prompt per line.
    """
    with NamedTemporaryFile(mode="w") as tmp_file:
        # Generate the required number of prompts.
        prompts = [
            {
                "modelInput": {
                    "anthropic_version": ANTHROPIC_VERSION,
                    "max_tokens": 1000,
                    "messages": [PROMPT_TEMPLATE.format(n=n)],
                },
            }
            for n in range(MIN_NUM_PROMPTS)
        ]

        # Convert each prompt to serialized json, append a newline, and write that line to the temp file.
        tmp_file.writelines(json.dumps(prompt) + "\n" for prompt in prompts)

        # Flush the buffer to ensure all data is written to disk before upload
        tmp_file.flush()

        # Upload the file to S3.
        S3Hook().conn.upload_file(tmp_file.name, _bucket, _key)


@task(trigger_rule=TriggerRule.ALL_DONE)
def stop_batch_inference(job_arn: str):
    log.info("Stopping Batch Inference Job.")
    try:
        BedrockHook().conn.stop_model_invocation_job(jobIdentifier=job_arn)
    except ClientError as e:
        # If the job has already completed, boto will raise a ValidationException.  Consider that a successful result.
        if (e.response["Error"]["Code"] == "ValidationException") and (
            "State was: Completed" in e.response["Error"]["Message"]
        ):
            pass


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags={"example"},
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    bucket_name = f"{env_id}-bedrock"
    input_data_s3_key = f"{env_id}/prompt_list.jsonl"
    input_uri = f"s3://{bucket_name}/{input_data_s3_key}"
    output_uri = f"s3://{bucket_name}/output/"
    job_name = f"batch-infer-{env_id}"

    # Test that this configuration works for a single prompt before trying the batch inferences.
    # [START howto_operator_invoke_claude_messages]
    invoke_claude_messages = BedrockInvokeModelOperator(
        task_id="invoke_claude_messages",
        model_id=CLAUDE_MODEL_ID,
        input_data={
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "messages": [{"role": "user", "content": PROMPT_TEMPLATE.format(n=42)}],
        },
    )
    # [END howto_operator_invoke_claude_messages]

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    # [START howto_operator_bedrock_batch_inference]
    batch_infer = BedrockBatchInferenceOperator(
        task_id="batch_infer",
        job_name=job_name,
        role_arn=test_context[ROLE_ARN_KEY],
        model_id=CLAUDE_MODEL_ID,
        input_uri=input_uri,
        output_uri=output_uri,
    )
    # [END howto_operator_bedrock_batch_inference]
    batch_infer.wait_for_completion = False
    batch_infer.deferrable = False

    # [START howto_sensor_bedrock_batch_inference_scheduled]
    await_job_scheduled = BedrockBatchInferenceSensor(
        task_id="await_job_scheduled",
        job_arn=batch_infer.output,
        success_state=BedrockBatchInferenceSensor.SuccessState.SCHEDULED,
    )
    # [END howto_sensor_bedrock_batch_inference_scheduled]

    stop_job = stop_batch_inference(batch_infer.output)

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        invoke_claude_messages,
        create_bucket,
        generate_prompts(env_id, bucket_name, input_data_s3_key),
        # TEST BODY
        batch_infer,
        await_job_scheduled,
        stop_job,
        # TEST TEARDOWN
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
