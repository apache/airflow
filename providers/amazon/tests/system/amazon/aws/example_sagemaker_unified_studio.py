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

import os
from datetime import datetime

import pytest

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import (
    SageMakerNotebookOperator,
)
from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS

"""
Prerequisites: The account which runs this test must manually have the following:
1. An IAM IDC organization set up in the testing region with a user initialized
2. A SageMaker Unified Studio Domain (with default VPC and roles)
3. A project within the SageMaker Unified Studio Domain
4. A notebook (test_notebook.ipynb) placed in the project's s3 path

This test will emulate a DAG run in the shared MWAA environment inside a SageMaker Unified Studio Project.
The setup tasks will set up the project and configure the test runnner to emulate an MWAA instance.
Then, the SageMakerNotebookOperator will run a test notebook. This should spin up a SageMaker training job, run the notebook, and exit successfully.
"""

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_2_10_PLUS, reason="Test requires Airflow 2.10+"
)

DAG_ID = "example_sagemaker_unified_studio"

# Externally fetched variables:
DOMAIN_ID_KEY = "DOMAIN_ID"
PROJECT_ID_KEY = "PROJECT_ID"
ENVIRONMENT_ID_KEY = "ENVIRONMENT_ID"
S3_PATH_KEY = "S3_PATH"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(DOMAIN_ID_KEY)
    .add_variable(PROJECT_ID_KEY)
    .add_variable(ENVIRONMENT_ID_KEY)
    .add_variable(S3_PATH_KEY)
    .build()
)


@task
def emulate_mwaa_environment(
    domain_id: str, project_id: str, environment_id: str, s3_path: str
):
    """
    Sets several environment variables in the container to emulate an MWAA environment provisioned
    within SageMaker Unified Studio.
    """
    AIRFLOW_PREFIX = "AIRFLOW__WORKFLOWS__"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_ID"] = domain_id
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_PROJECT_ID"] = project_id
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_ENVIRONMENT_ID"] = environment_id
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_SCOPE_NAME"] = "dev"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_STAGE"] = "prod"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_ENDPOINT"] = (
        "https://datazone.us-east-1.api.aws"
    )
    os.environ[f"{AIRFLOW_PREFIX}PROJECT_S3_PATH"] = s3_path
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_REGION"] = "us-east-1"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()

    test_env_id = test_context[ENV_ID_KEY]
    domain_id = test_context[DOMAIN_ID_KEY]
    project_id = test_context[PROJECT_ID_KEY]
    environment_id = test_context[ENVIRONMENT_ID_KEY]
    s3_path = test_context[S3_PATH_KEY]

    setup_mock_mwaa_environment = emulate_mwaa_environment(
        domain_id,
        project_id,
        environment_id,
        s3_path,
    )

    # [START howto_operator_sagemaker_unified_studio_notebook]
    notebook_path = "test_notebook.ipynb"  # This should be the path to your .ipynb, .sqlnb, or .vetl file in your project.

    run_notebook = SageMakerNotebookOperator(
        task_id="run_notebook",
        input_config={"input_path": notebook_path, "input_params": {}},
        output_config={"output_formats": ["NOTEBOOK"]},  # optional
        compute={
            "InstanceType": "ml.m5.large",
            "VolumeSizeInGB": 30,
        },  # optional
        termination_condition={"MaxRuntimeInSeconds": 600},  # optional
        tags={},  # optional
        wait_for_completion=True,  # optional
        waiter_delay=5,  # optional
        deferrable=False,  # optional
    )
    # [END howto_operator_sagemaker_unified_studio_notebook]

    chain(
        # TEST SETUP
        test_context,
        setup_mock_mwaa_environment,
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
