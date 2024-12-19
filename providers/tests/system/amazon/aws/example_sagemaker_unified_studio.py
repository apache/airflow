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
import pytest

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import SageMakerNotebookOperator

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS

"""
Prerequisites: The account which runs this test must manually have the following:
1. An IAM IDC organization set up in the testing region with the following user initialized:
    Username: airflowTestUser
    Password: airflowSystemTestP@ssword1!
"""

pytestmark = pytest.mark.skipif(not AIRFLOW_V_2_10_PLUS, reason="Test requires Airflow 2.10+")

DAG_ID = "example_sagemaker_unified_studio"

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_roles_and_network_stack():
    """
    Creates SageMakerUnifiedStudio VPC and IAM roles for a quick-setup SageMaker Unified Studio
    domain and project. Does so by running a CloudFormation template.
    Returns domainExecutionRole for domain creation.
    """
    pass


@task
def create_sus_domain(env_id: str, domain_execution_role: str):
    """
    Creates a SageMaker Unified Studio Domain in the testing account.
    This assumes network and IDC prerequisites have been fulfilled.
    Returns domain id and environment id.
    """
    import time

    # 1. Create DataZone domain
    datazone_client = boto3.client("datazone")
    create_domain_response = datazone_client.create_domain(
        clientToken=env_id,
        domainExecutionRole=domain_execution_role,
        name=f"sus-domain-{env_id}",
        singleSignOn={"type": "IAM_IDC", "userAssignment": "AUTOMATIC"},
    )
    domain_id = create_domain_response["id"]

    retries = 0
    while status := datazone_client.get_domain(identifier=domain_id) != "AVAILABLE":
        retries += 1
        if retries > 3:
            raise RuntimeError("Create domain timed out")
        print(f"Creating DataZone domain {domain_id}, Status = {status}")
        time.sleep(10)  # polling every 10 seconds

    # 2. Add Environment blueprints
    # TODO: get the VPC/subnets for SUS and add them to blueprints
    # TODO: create the data analytics profile with tooling blueprint (only one we need?)
    create_project_profile_response = datazone_client.create_project_profile(data_analytics_profile)

    # TODO: figure out how to get environment id
    # TODO: disambiguate between the env_id for this run and the datazone environment_id
    return {"domain_id": domain_id}


@task
def create_sus_project(domain_id: str):
    """
    Creates a project within a given SageMaker Unified Studio Domain.
    Returns project id.
    """
    datazone_client = boto3.client("datazone")
    create_project_response = datazone_client.create_project(domainIdentifier=domain_id)
    # TODO: figure out how to create this project with the project profile we need

    return {"project_id": create_project_response["id"]}


@task
def emulate_mwaa_environment():
    """
    Sets several environment variables in the container to emulate an MWAA environment provisioned
    within SageMaker Unified Studio.
    """
    pass


@task
def delete_sus_domain(env_id: str, domain_id: str):
    """
    Deletes the SageMaker Unified Studio Domain at the given domain id.
    This also deletes any projects hosted within that domain.
    """
    datazone_client = boto3.client("datazone")
    delete_domain_response = datazone_client.delete_domain(client_token=env_id, identifier=domain_id)

    # TODO: check for successful deletion
    return delete_domain_response


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    notebook_path = "tests/amazon/aws/operators/test_notebook.ipynb"

    setup_sus_domain = create_sus_domain(env_id=env_id)

    setup_sus_project = create_sus_project(domain_id=setup_sus_domain["domain_id"])

    setup_mock_mwaa_environment = emulate_mwaa_environment()

    run_notebook = SageMakerNotebookOperator(
        task_id="initial",
        input_config={"input_path": notebook_path, "input_params": {}},
        output_config={"output_formats": ["NOTEBOOK"]},
        wait_for_completion=True,
        poll_interval=5,
    )

    teardown_sus_domain = delete_sus_domain(domain_id=setup_sus_domain["domain_id"])

    chain(
        # TEST SETUP
        test_context,
        setup_sus_domain,
        setup_sus_project,
        setup_mock_mwaa_environment,
        # TEST BODY
        run_notebook,
        # TEST TEARDOWN
        teardown_sus_domain,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
