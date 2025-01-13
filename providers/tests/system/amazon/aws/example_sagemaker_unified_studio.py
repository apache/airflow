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
import os

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.version_compat import AIRFLOW_V_2_10_PLUS

"""
Prerequisites: The account which runs this test must manually have the following:
1. An IAM IDC organization set up in the testing region with the following user initialized:
    Username: airflowTestUser
    Password: airflowSystemTestP@ssword1!

Essentially, this test will emulate a DAG run in the shared MWAA environment inside a SageMaker Unified Studio Project.
The setup tasks will set up the project and configure the test runnner to emulate an MWAA instance.
Then, the SageMakerNotebookOperator will run a test notebook. This should spin up a SageMaker training job, run the notebook, and exit successfully.
The teardown tasks will finally delete the project and domain that was set up for this test run.
"""

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_2_10_PLUS, reason="Test requires Airflow 2.10+")

DAG_ID = "example_sagemaker_unified_studio"

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_roles_and_network_stack():
    """
    Creates SageMakerUnifiedStudio VPC and IAM roles for a quick-setup SageMaker Unified Studio
    domain and project. Does so by running a CloudFormation template.
    Returns domainExecutionRole, manageAccessRole, provisioningRole for domain creation.
    """
    # TODO: do we still need the CFN template or will roles be created automatically now?
    pass


@task
def create_sus_domain(
    env_id: str, domain_execution_role: str, manage_access_role: str, provisioning_role: str
):
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
    while status := datazone_client.get_domain(identifier=domain_id)["status"] != "AVAILABLE":
        retries += 1
        if retries > 3:
            raise RuntimeError("Create domain timed out")
        print(f"Creating DataZone domain {domain_id}, Status = {status}")
        time.sleep(10)  # polling every 10 seconds

    # 2. Add Environment blueprints and create project profile
    data_analytics_profile = construct_data_analytics_profile(
        datazone_client,
        domain_id,
        manage_access_role_arn=manage_access_role,
        provisioning_role_arn=provisioning_role,
    )
    create_project_profile_response = datazone_client.create_project_profile(
        **data_analytics_profile)

    return {"domain_id": domain_id}


def get_common_config(blueprint):
    account = None
    region = None
    return {
        "awsAccount": {"awsAccountId": account},
        "awsRegion": {"regionName": region},
        "configurationParameters": {},  # user default parameters
        "description": blueprint["description"],
        "environmentBlueprintId": blueprint["id"],
        "name": blueprint["name"],
    }


def construct_data_analytics_profile(
    datazone_client: boto3.Client, domain_id: str, manage_access_role_arn: str, provisioning_role_arn: str
):
    # 1. get blueprints
    environment_blueprints = datazone_client.list_environment_blueprints(
        domainIdentifier=domain_id,
        managed=True,
    )["items"]
    environment_blueprints = list(
        filter(lambda x: x["provider"] ==
               "Amazon SageMaker", environment_blueprints)
    )
    # TODO: 2. get subnet data and regional params
    enabled_regions = None
    regional_params = None
    # 3. update blueprint with regional params
    for blueprint in environment_blueprints:
        datazone_client.put_environment_blueprint_configurations(
            domainIdentifier=domain_id,
            environmentBlueprintIdentifier=blueprint["id"],
            enabledRegions=enabled_regions,  # from network step 2
            manageAccessRoleArn=manage_access_role_arn,
            provisioningRoleArn=provisioning_role_arn,
            regionalParameters=regional_params,  # from network step 2
        )
    # 4. construct tooling configs
    tooling_blueprint = next(
        (x for x in environment_blueprints if x["name"] == "Tooling"), None)
    datalake_blueprint = next(
        (x for x in environment_blueprints if x["name"] == "DataLake"), None)
    lakehouse_catalog_blueprint = next(
        (x for x in environment_blueprints if x["name"]
         == "LakehouseCatalog"), None
    )
    redshift_serverless_blueprint = next(
        (x for x in environment_blueprints if x["name"]
         == "RedshiftServerless"), None
    )
    emr_serverless_blueprint = next(
        (x for x in environment_blueprints if x["name"] == "EmrServerless"), None)
    emr_on_ec2_blueprint = next(
        (x for x in environment_blueprints if x["name"] == "EmrOnEc2"), None)

    tooling_config = {
        **get_common_config(tooling_blueprint),
        "configurationParameters": {},
        "deploymentMode": "ON_CREATE",
        "deploymentOrder": 0,
    }
    datalake_config = {
        **get_common_config(datalake_blueprint), "deploymentMode": "ON_CREATE"}

    lakehouse_catalog_config = {
        **get_common_config(lakehouse_catalog_blueprint),
        "deploymentMode": "ON_DEMAND",
    }

    redshift_serverless_on_create_config = {
        **get_common_config(redshift_serverless_blueprint),
        "deploymentMode": "ON_CREATE",
    }

    redshift_serverless_on_demand_config = {
        **redshift_serverless_on_create_config,
        "configurationParameters": {
            "parameterOverrides": [
                {"name": "connectionName", "isEditable": True},
                {"name": "connectionDescription", "isEditable": True},
            ]
        },
        "deploymentMode": "ON_DEMAND",
    }

    emr_serverless_config = {
        **get_common_config(emr_serverless_blueprint), "deploymentMode": "ON_DEMAND"}

    emr_on_ec2_config = {
        **get_common_config(emr_on_ec2_blueprint), "deploymentMode": "ON_DEMAND"}
    # 5. place required configs in data analytics profile
    profile_name_suffix = None
    data_analytics_profile = {
        # TODO: pull this name suffix from somewhere (check original)
        "name": f"Data analytics project profile{profile_name_suffix}",
        "description": "This sample project profile creates environments with necessary AWS resources and tooling for data lake activities. It includes AWS Glue databases, Amazon Athena workgroups, Lakehouse catalog, Amazon Redshift Serverless workgroups, and more.",
        "domainIdentifier": domain_id,
        "environmentConfigurations": [
            tooling_config,
            datalake_config,
            redshift_serverless_on_create_config,
            emr_on_ec2_config,
            redshift_serverless_on_demand_config,
            lakehouse_catalog_config,
            emr_serverless_config,
        ],
        "status": "ENABLED",
    }

    return data_analytics_profile


@task
def create_sus_project(domain_id: str, project_profile_id: str):
    """
    Creates a project within a given SageMaker Unified Studio Domain.
    Returns project id.
    """
    import time

    datazone_client = boto3.client("datazone")
    create_project_response = datazone_client.create_project(
        domainIdentifier=domain_id,
        name="testProject",
        projectProfileId=project_profile_id,
    )
    project_id = create_project_response["id"]
    # Wait for project to be created. Can take up to 10 mins
    retries = 0
    while (
        project_status := datazone_client.get_project(domainIdentifier=domain_id, identifier=project_id)[
            "projectStatus"
        ]
        != "ACTIVE"
    ) | (
        environment_status := datazone_client.get_project(domainIdentifier=domain_id, identifier=project_id)[
            "environmentDeploymentDetails"
        ]["overallDeploymentStatus"]
        != "SUCCESSFUL"
    ):
        retries += 1
        if retries > 20:
            raise RuntimeError("Create project timed out")
        print(
            f"Creating DataZone project {project_id}, project status = {project_status} and environment status = {environment_status}"
        )
        time.sleep(30)  # polling every 10 seconds

    return {
        "project_id": project_id,
        "environment_id": min(
            datazone_client.list_environments(domainIdentifier=domain_id, projectIdentifier=project_id)[
                "items"
            ],
            key=lambda x: x["deploymentOrder"],
        )["id"],
    }


@task
def emulate_mwaa_environment():
    """
    Sets several environment variables in the container to emulate an MWAA environment provisioned
    within SageMaker Unified Studio.
    """
    print(os.getcwd())
    AIRFLOW_PREFIX = "AIRFLOW__WORKFLOWS__"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_ID"] = "dzd_5i3k33tlvculbb"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_PROJECT_ID"] = "6g0ib1wbhy1o6f"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_ENVIRONMENT_ID"] = "45309kaqlx06qv"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_SCOPE_NAME"] = "dev"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_STAGE"] = "prod"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_ENDPOINT"] = "https://datazone.us-east-1.api.aws"
    os.environ[f"{AIRFLOW_PREFIX}PROJECT_S3_PATH"] = "s3://amazon-sagemaker-713881818467-us-east-1-5f2b2dc83db7/dzd_5i3k33tlvculbb/6g0ib1wbhy1o6f/dev"
    os.environ[f"{AIRFLOW_PREFIX}DATAZONE_DOMAIN_REGION"] = "us-east-1"


@task
def delete_sus_domain(env_id: str, domain_id: str):
    """
    Deletes the SageMaker Unified Studio Domain at the given domain id.
    This also deletes any projects hosted within that domain.
    """
    datazone_client = boto3.client("datazone")
    delete_domain_response = datazone_client.delete_domain(
        client_token=env_id, identifier=domain_id)

    # TODO: check for successful deletion
    return delete_domain_response


def notebook_operator():
    # from airflow.providers.amazon.aws.operators.sagemaker_unified_studio import SageMakerNotebookOperator
    # return SageMakerNotebookOperator(
    #     task_id="initial",
    #     input_config={"input_path": notebook_path, "input_params": {}},
    #     output_config={"output_formats": ["NOTEBOOK"]},
    #     wait_for_completion=True,
    #     poll_interval=5,
    # )
    return BashOperator(
        task_id="test",
        bash_command="printenv",
    )

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

    # setup_sus_domain = create_sus_domain(env_id=env_id)

    # setup_sus_project = create_sus_project(
    #     domain_id=setup_sus_domain["domain_id"])

    setup_mock_mwaa_environment = emulate_mwaa_environment()

    run_notebook = notebook_operator()

    # teardown_sus_domain = delete_sus_domain(
    #     domain_id=setup_sus_domain["domain_id"])

    chain(
        # TEST SETUP
        test_context,
        # setup_sus_domain,
        # setup_sus_project,
        setup_mock_mwaa_environment,
        # TEST BODY
        run_notebook,
        # TEST TEARDOWN
        # teardown_sus_domain,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
test_run = get_test_run(dag)
