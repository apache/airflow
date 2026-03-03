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

from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import (
    SageMakerNotebookOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

"""
Prerequisites: The account which runs this test must manually have the following:
1. An IAM IDC organization set up in the testing region with a user initialized
2. A SageMaker Unified Studio Domain (with default VPC and roles)
3. A project within the SageMaker Unified Studio Domain
4. A notebook (test_notebook.ipynb) placed in the project's s3 path

This test will emulate a DAG run in the shared MWAA environment inside a SageMaker Unified Studio Project.
The setup tasks will set up the project and configure the test runner to emulate an MWAA instance.
Then, the SageMakerNotebookOperator will run a test notebook. This should spin up a SageMaker training job, run the notebook, and exit successfully.
"""

DAG_ID = "example_sagemaker_unified_studio"

# Externally fetched variables:
DOMAIN_ID_KEY = "DOMAIN_ID"
PROJECT_ID_KEY = "PROJECT_ID"
ENVIRONMENT_ID_KEY = "ENVIRONMENT_ID"
S3_PATH_KEY = "S3_PATH"
REGION_NAME_KEY = "REGION_NAME"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(DOMAIN_ID_KEY)
    .add_variable(PROJECT_ID_KEY)
    .add_variable(ENVIRONMENT_ID_KEY)
    .add_variable(S3_PATH_KEY)
    .add_variable(REGION_NAME_KEY)
    .build()
)


def get_mwaa_environment_params(
    domain_id: str,
    project_id: str,
    environment_id: str,
    s3_path: str,
    region_name: str,
):
    AIRFLOW_PREFIX = "AIRFLOW__WORKFLOWS__"

    parameters = {}
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_ID"] = domain_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_PROJECT_ID"] = project_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_ENVIRONMENT_ID"] = environment_id
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_SCOPE_NAME"] = "dev"
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_STAGE"] = "prod"
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_ENDPOINT"] = f"https://datazone.{region_name}.api.aws"
    parameters[f"{AIRFLOW_PREFIX}PROJECT_S3_PATH"] = s3_path
    parameters[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_REGION"] = region_name
    return parameters


@task
def mock_mwaa_environment(parameters: dict):
    """
    Sets several environment variables in the container to emulate an MWAA environment provisioned
    within SageMaker Unified Studio. When running in the ECSExecutor, this is a no-op.
    """
    import os

    for key, value in parameters.items():
        os.environ[key] = value


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_env_id = test_context[ENV_ID_KEY]
    domain_id = test_context[DOMAIN_ID_KEY]
    project_id = test_context[PROJECT_ID_KEY]
    environment_id = test_context[ENVIRONMENT_ID_KEY]
    s3_path = test_context[S3_PATH_KEY]
    region_name = test_context[REGION_NAME_KEY]

    mock_mwaa_environment_params = get_mwaa_environment_params(
        domain_id,
        project_id,
        environment_id,
        s3_path,
        region_name,
    )

    setup_mwaa_environment = mock_mwaa_environment(mock_mwaa_environment_params)

    # [START howto_operator_sagemaker_unified_studio_notebook]
    notebook_path = "test_notebook.ipynb"  # This should be the path to your .ipynb, .sqlnb, or .vetl file in your project.

    run_notebook = SageMakerNotebookOperator(
        task_id="run-notebook",
        input_config={"input_path": notebook_path, "input_params": {}},
        output_config={"output_formats": ["NOTEBOOK"]},  # optional
        compute={
            "instance_type": "ml.m5.large",
            "volume_size_in_gb": 30,
        },  # optional
        termination_condition={"max_runtime_in_seconds": 600},  # optional
        tags={},  # optional
        wait_for_completion=True,  # optional
        waiter_delay=5,  # optional
        deferrable=False,  # optional
        executor_config={  # optional
            "overrides": {
                "containerOverrides": [
                    {
                        "environment": [
                            {"name": key, "value": value}
                            for key, value in mock_mwaa_environment_params.items()
                        ],
                        "name": "ECSExecutorContainer",  # Necessary parameter
                    }
                ]
            }
        },
    )
    # [END howto_operator_sagemaker_unified_studio_notebook]

    chain(
        # TEST SETUP
        test_context,
        setup_mwaa_environment,
        # TEST BODY
        run_notebook,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
