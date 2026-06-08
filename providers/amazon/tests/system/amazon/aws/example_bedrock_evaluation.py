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

from airflow.providers.amazon.aws.operators.bedrock import BedrockCreateEvaluationJobOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_bedrock_evaluation"

ROLE_ARN_KEY = "ROLE_ARN"
sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

# Minimal JSONL dataset for Summarization task
EVAL_DATASET = (
    '{"prompt": "Summarize: The quick brown fox jumps over the lazy dog.",'
    ' "referenceResponse": "A fox jumps over a dog."}\n'
)

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    bucket_name = "airflow-system-test-bedrock-eval"

    # TEST SETUP
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    upload_dataset = S3CreateObjectOperator(
        task_id="upload_dataset",
        s3_bucket=bucket_name,
        s3_key="datasets/summarization.jsonl",
        data=EVAL_DATASET,
    )

    # TEST BODY
    # [START howto_operator_bedrock_create_evaluation_job]
    create_evaluation_job = BedrockCreateEvaluationJobOperator(
        task_id="create_evaluation_job",
        job_name=f"{env_id}-eval",
        role_arn=role_arn,
        evaluation_config={
            "automated": {
                "datasetMetricConfigs": [
                    {
                        "taskType": "Summarization",
                        "dataset": {
                            "name": "eval-dataset",
                            "datasetLocation": {"s3Uri": f"s3://{bucket_name}/datasets/summarization.jsonl"},
                        },
                        "metricNames": ["Builtin.Accuracy"],
                    }
                ]
            }
        },
        inference_config={
            "models": [
                {
                    "bedrockModel": {
                        "modelIdentifier": "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                        "inferenceParams": "{}",
                    }
                }
            ]
        },
        output_data_config={"s3Uri": f"s3://{bucket_name}/output/"},
    )
    # [END howto_operator_bedrock_create_evaluation_job]

    # TEST TEARDOWN
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def stop_evaluation_job(job_arn: str):
        import contextlib

        import boto3

        with contextlib.suppress(Exception):
            boto3.client("bedrock").stop_evaluation_job(jobIdentifier=job_arn)

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        upload_dataset,
        # TEST BODY
        create_evaluation_job,
        # TEST TEARDOWN
        stop_evaluation_job(create_evaluation_job.output),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
