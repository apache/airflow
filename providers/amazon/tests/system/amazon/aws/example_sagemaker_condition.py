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
"""
System test for SageMakerConditionOperator.

This operator evaluates conditions against XCom values passed from upstream tasks.

The DAG simulates an ML accuracy-gate workflow:

1. ``produce_metrics`` pushes a dict of metrics to XCom.
2. ``check_accuracy`` uses SageMakerConditionOperator to branch:
   - accuracy >= 0.9 AND loss < 0.1 -> ``deploy_model``
   - otherwise -> ``retrain_model``
3. Only the correct branch task runs; the other is skipped.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.amazon.aws.operators.sagemaker import SageMakerConditionOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_sagemaker_condition"

sys_test_context_task = SystemTestContextBuilder().build()


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    # TEST SETUP: push simulated ML metrics to XCom

    @task
    def produce_metrics():
        """Simulate an ML training job that returns accuracy and loss metrics."""
        return {"accuracy": 0.95, "loss": 0.04}

    metrics = produce_metrics()

    # TEST BODY

    # [START howto_operator_sagemaker_condition]
    check_accuracy = SageMakerConditionOperator(
        task_id="check_accuracy",
        conditions=[
            {
                "type": "GreaterThanOrEqualTo",
                "left_value": "{{ ti.xcom_pull(task_ids='produce_metrics')['accuracy'] }}",
                "right_value": 0.9,
            },
            {
                "type": "LessThan",
                "left_value": "{{ ti.xcom_pull(task_ids='produce_metrics')['loss'] }}",
                "right_value": 0.1,
            },
        ],
        if_task_ids=["deploy_model"],
        else_task_ids=["retrain_model"],
    )
    # [END howto_operator_sagemaker_condition]

    @task
    def deploy_model():
        """Placeholder: model meets quality bar, proceed to deployment."""
        return "deployed"

    @task
    def retrain_model():
        """Placeholder: model does not meet quality bar, retrain."""
        return "retrained"

    # Scenario 2: condition evaluates to False -> else branch

    @task
    def produce_bad_metrics():
        """Simulate a training job with poor accuracy."""
        return {"accuracy": 0.5, "loss": 0.8}

    bad_metrics = produce_bad_metrics()

    # [START howto_operator_sagemaker_condition_flat]
    check_bad_accuracy = SageMakerConditionOperator(
        task_id="check_bad_accuracy",
        condition_type="GreaterThanOrEqualTo",
        left_value="{{ ti.xcom_pull(task_ids='produce_bad_metrics')['accuracy'] }}",
        right_value=0.9,
        if_task_ids=["should_not_run"],
        else_task_ids=["should_run"],
    )
    # [END howto_operator_sagemaker_condition_flat]

    @task
    def should_not_run():
        """This task should be skipped because accuracy < 0.9."""
        return "error: should not have run"

    @task
    def should_run():
        """This task should execute because accuracy < 0.9 -> else branch."""
        return "correctly routed to else branch"

    # Scenario 3: Or condition + Not condition

    # [START howto_operator_sagemaker_condition_not_or]
    check_logical = SageMakerConditionOperator(
        task_id="check_logical",
        conditions=[
            {
                "type": "Or",
                "conditions": [
                    {"type": "Equals", "left_value": 1, "right_value": 2},
                    {"type": "Equals", "left_value": 3, "right_value": 3},
                ],
            },
            {
                "type": "Not",
                "condition": {"type": "Equals", "left_value": "a", "right_value": "b"},
            },
        ],
        if_task_ids=["logical_pass"],
        else_task_ids=["logical_fail"],
    )
    # [END howto_operator_sagemaker_condition_not_or]

    @task
    def logical_pass():
        """Or(1==2, 3==3) -> True AND Not(a==b) -> True -> if branch."""
        return "logical conditions passed"

    @task
    def logical_fail():
        return "error: logical conditions should have passed"

    test_context >> [metrics, bad_metrics, check_logical]

    chain(metrics, check_accuracy, [deploy_model(), retrain_model()])
    chain(bad_metrics, check_bad_accuracy, [should_not_run(), should_run()])
    chain(check_logical, [logical_pass(), logical_fail()])

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
