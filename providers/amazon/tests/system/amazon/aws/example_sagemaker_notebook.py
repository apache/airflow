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

from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerCreateNotebookOperator,
    SageMakerDeleteNotebookOperator,
    SageMakerStartNoteBookOperator,
    SageMakerStopNotebookOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_sagemaker_notebook"

# Externally fetched variables:
ROLE_ARN_KEY = "ROLE_ARN"  # must have an IAM role to run notebooks

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    instance_name: str = f"{test_context[ENV_ID_KEY]}-test-notebook"

    role_arn = test_context[ROLE_ARN_KEY]

    # [START howto_operator_sagemaker_notebook_create]
    instance = SageMakerCreateNotebookOperator(
        task_id="create_instance",
        instance_name=instance_name,
        instance_type="ml.t3.medium",
        role_arn=role_arn,
        wait_for_completion=True,
    )
    # [END howto_operator_sagemaker_notebook_create]

    # [START howto_operator_sagemaker_notebook_stop]
    stop_instance = SageMakerStopNotebookOperator(
        task_id="stop_instance",
        instance_name=instance_name,
    )
    # [END howto_operator_sagemaker_notebook_stop]

    # [START howto_operator_sagemaker_notebook_start]
    start_instance = SageMakerStartNoteBookOperator(
        task_id="start_instance",
        instance_name=instance_name,
    )

    # [END howto_operator_sagemaker_notebook_start]

    # Instance must be stopped before it can be deleted.
    stop_instance_before_delete = SageMakerStopNotebookOperator(
        task_id="stop_instance_before_delete",
        instance_name=instance_name,
    )
    # [START howto_operator_sagemaker_notebook_delete]
    delete_instance = SageMakerDeleteNotebookOperator(task_id="delete_instance", instance_name=instance_name)
    # [END howto_operator_sagemaker_notebook_delete]

    chain(
        test_context,
        # create a new instance
        instance,
        # stop the instance
        stop_instance,
        # restart the instance
        start_instance,
        # must stop before deleting
        stop_instance_before_delete,
        # delete the instance
        delete_instance,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
