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

from airflow.providers.amazon.aws.operators.mwaa_serverless import (
    MwaaServerlessCreateWorkflowOperator,
    MwaaServerlessStartWorkflowRunOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.mwaa_serverless import MwaaServerlessWorkflowRunSensor
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

DAG_ID = "example_mwaa_serverless"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"

# Valid MWAA Serverless YAML: tasks as mapping, FQN operators, flat parameters.
WORKFLOW_YAML = """\
systest_mwaa_serverless:
  schedule: null
  description: "System test: S3 key sensor on workflow definition"
  tasks:
    check_file:
      task_id: check_file
      operator: airflow.providers.amazon.aws.sensors.s3.S3KeySensor
      bucket_name: {bucket}
      bucket_key: workflow.yaml
"""

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()


@task(trigger_rule=TriggerRule.ALL_DONE)
def stop_workflow_run(workflow_arn: str, run_id: str):
    """Stop the workflow run."""
    import boto3

    boto3.client("mwaa-serverless").stop_workflow_run(WorkflowArn=workflow_arn, RunId=run_id)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_workflow(workflow_arn: str):
    """Delete the MWAA Serverless workflow."""
    import boto3

    boto3.client("mwaa-serverless").delete_workflow(WorkflowArn=workflow_arn)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    bucket_name = f"{env_id}-mwaa-sl"

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    upload_workflow_yaml = S3CreateObjectOperator(
        task_id="upload_workflow_yaml",
        s3_bucket=bucket_name,
        s3_key="workflow.yaml",
        data=WORKFLOW_YAML.format(bucket=bucket_name),
    )

    # [START howto_operator_mwaa_serverless_create_workflow]
    create_workflow = MwaaServerlessCreateWorkflowOperator(
        task_id="create_workflow",
        workflow_name=bucket_name,
        definition_s3_location={"Bucket": bucket_name, "ObjectKey": "workflow.yaml"},
        role_arn=role_arn,
    )
    # [END howto_operator_mwaa_serverless_create_workflow]

    workflow_arn = create_workflow.output

    # [START howto_operator_mwaa_serverless_start_workflow_run]
    start_workflow = MwaaServerlessStartWorkflowRunOperator(
        task_id="start_workflow",
        workflow_arn=workflow_arn,
    )
    # [END howto_operator_mwaa_serverless_start_workflow_run]

    # [START howto_sensor_mwaa_serverless_workflow_run]
    wait_for_run = MwaaServerlessWorkflowRunSensor(
        task_id="wait_for_run",
        workflow_arn=workflow_arn,
        run_id=start_workflow.output,
        poke_interval=30,
        timeout=600,
    )
    # [END howto_sensor_mwaa_serverless_workflow_run]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        test_context,
        create_bucket,
        upload_workflow_yaml,
        workflow_arn,
        start_workflow,
        wait_for_run,
        stop_workflow_run(workflow_arn=workflow_arn, run_id=start_workflow.output),
        delete_workflow(workflow_arn=workflow_arn),
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
