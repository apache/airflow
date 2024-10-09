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
import subprocess
from datetime import datetime

import boto3

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import EksCreateClusterOperator, EksDeleteClusterOperator
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator, EmrEksCreateClusterOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_emr_eks"

# Externally fetched variables
ROLE_ARN_KEY = "ROLE_ARN"
JOB_ROLE_ARN_KEY = "JOB_ROLE_ARN"
JOB_ROLE_NAME_KEY = "JOB_ROLE_NAME"
SUBNETS_KEY = "SUBNETS"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(ROLE_ARN_KEY)
    .add_variable(JOB_ROLE_ARN_KEY)
    .add_variable(JOB_ROLE_NAME_KEY)
    .add_variable(SUBNETS_KEY, split_string=True)
    .build()
)

S3_FILE_NAME = "pi.py"
S3_FILE_CONTENT = """
k = 1
s = 0

for i in range(1000000):
    if i % 2 == 0:
        s += 4/k
    else:
        s -= 4/k

    k += 2

print(s)
"""


@task
def create_launch_template(template_name: str):
    # This launch template enables IMDSv2.
    boto3.client("ec2").create_launch_template(
        LaunchTemplateName=template_name,
        LaunchTemplateData={
            "MetadataOptions": {"HttpEndpoint": "enabled", "HttpTokens": "required"},
        },
    )


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_launch_template(template_name: str):
    boto3.client("ec2").delete_launch_template(LaunchTemplateName=template_name)


@task
def run_eksctl_commands(cluster_name, ns):
    # Install eksctl and enable access for EMR on EKS
    # See https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-cluster-access.html
    file = "https://github.com/weaveworks/eksctl/releases/latest/download/eksctl_$(uname -s)_amd64.tar.gz"
    commands = f"""
        curl --silent --location "{file}" | tar xz -C . &&
        ./eksctl create iamidentitymapping --cluster {cluster_name} --namespace {ns} --service-name "emr-containers" &&
        ./eksctl utils associate-iam-oidc-provider --cluster {cluster_name} --approve
    """

    build = subprocess.Popen(
        commands,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _, err = build.communicate()

    if build.returncode != 0:
        raise RuntimeError(err)


@task
def delete_iam_oidc_identity_provider(cluster_name):
    oidc_provider_issuer_url = boto3.client("eks").describe_cluster(
        name=cluster_name,
    )["cluster"]["identity"]["oidc"]["issuer"]
    oidc_provider_issuer_endpoint = oidc_provider_issuer_url.replace("https://", "")

    account_id = boto3.client("sts").get_caller_identity()["Account"]
    boto3.client("iam").delete_open_id_connect_provider(
        OpenIDConnectProviderArn=f"arn:aws:iam::{account_id}:oidc-provider/{oidc_provider_issuer_endpoint}"
    )


@task
def update_trust_policy_execution_role(cluster_name, cluster_namespace, role_name):
    # Remove any already existing trusted entities added with "update-role-trust-policy"
    # Prevent getting an error "Cannot exceed quota for ACLSizePerRole"
    client = boto3.client("iam")
    role_trust_policy = client.get_role(RoleName=role_name)["Role"]["AssumeRolePolicyDocument"]
    # We assume if the action is sts:AssumeRoleWithWebIdentity, the statement had been added with
    # "update-role-trust-policy". Removing it to not exceed the quota
    role_trust_policy["Statement"] = [
        statement
        for statement in role_trust_policy["Statement"]
        if statement["Action"] != "sts:AssumeRoleWithWebIdentity"
    ]

    client.update_assume_role_policy(
        RoleName=role_name,
        PolicyDocument=json.dumps(role_trust_policy),
    )

    # See https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/setting-up-trust-policy.html
    # The action "update-role-trust-policy" is not available in boto3, thus we need to do it using AWS CLI
    commands = (
        f"aws emr-containers update-role-trust-policy --cluster-name {cluster_name} "
        f"--namespace {cluster_namespace} --role-name {role_name}"
    )

    build = subprocess.Popen(
        commands,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _, err = build.communicate()

    if build.returncode != 0:
        raise RuntimeError(err)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_virtual_cluster(virtual_cluster_id):
    boto3.client("emr-containers").delete_virtual_cluster(
        id=virtual_cluster_id,
    )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    role_arn = test_context[ROLE_ARN_KEY]
    subnets = test_context[SUBNETS_KEY]
    job_role_arn = test_context[JOB_ROLE_ARN_KEY]
    job_role_name = test_context[JOB_ROLE_NAME_KEY]

    s3_bucket_name = f"{env_id}-bucket"
    eks_cluster_name = f"{env_id}-cluster"
    virtual_cluster_name = f"{env_id}-virtual-cluster"
    nodegroup_name = f"{env_id}-nodegroup"
    eks_namespace = "default"
    launch_template_name = f"{env_id}-launch-template"

    # [START howto_operator_emr_eks_config]
    job_driver_arg = {
        "sparkSubmitJobDriver": {
            "entryPoint": f"s3://{s3_bucket_name}/{S3_FILE_NAME}",
            "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G "
            "--conf spark.executor.cores=2 --conf spark.driver.cores=1",
        }
    }

    configuration_overrides_arg = {
        "monitoringConfiguration": {
            "cloudWatchMonitoringConfiguration": {
                "logGroupName": "/emr-eks-jobs",
                "logStreamNamePrefix": "airflow",
            }
        },
    }
    # [END howto_operator_emr_eks_config]

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=s3_bucket_name,
    )

    upload_s3_file = S3CreateObjectOperator(
        task_id="upload_s3_file",
        s3_bucket=s3_bucket_name,
        s3_key=S3_FILE_NAME,
        data=S3_FILE_CONTENT,
    )

    create_cluster_and_nodegroup = EksCreateClusterOperator(
        task_id="create_cluster_and_nodegroup",
        cluster_name=eks_cluster_name,
        nodegroup_name=nodegroup_name,
        cluster_role_arn=role_arn,
        # Opting to use the same ARN for the cluster and the nodegroup here,
        # but a different ARN could be configured and passed if desired.
        nodegroup_role_arn=role_arn,
        resources_vpc_config={"subnetIds": subnets},
        # The launch template enforces IMDSv2 and is required for internal
        # compliance when running these system tests on AWS infrastructure.
        create_nodegroup_kwargs={"launchTemplate": {"name": launch_template_name}},
    )

    await_create_nodegroup = EksNodegroupStateSensor(
        task_id="await_create_nodegroup",
        cluster_name=eks_cluster_name,
        nodegroup_name=nodegroup_name,
        target_state=NodegroupStates.ACTIVE,
        poke_interval=10,
    )

    # [START howto_operator_emr_eks_create_cluster]
    create_emr_eks_cluster = EmrEksCreateClusterOperator(
        task_id="create_emr_eks_cluster",
        virtual_cluster_name=virtual_cluster_name,
        eks_cluster_name=eks_cluster_name,
        eks_namespace=eks_namespace,
    )
    # [END howto_operator_emr_eks_create_cluster]

    # [START howto_operator_emr_container]
    job_starter = EmrContainerOperator(
        task_id="start_job",
        virtual_cluster_id=str(create_emr_eks_cluster.output),
        execution_role_arn=job_role_arn,
        release_label="emr-7.0.0-latest",
        job_driver=job_driver_arg,
        configuration_overrides=configuration_overrides_arg,
        name="pi.py",
    )
    # [END howto_operator_emr_container]
    job_starter.wait_for_completion = False
    job_starter.job_retry_max_attempts = 5

    # [START howto_sensor_emr_container]
    job_waiter = EmrContainerSensor(
        task_id="job_waiter",
        virtual_cluster_id=str(create_emr_eks_cluster.output),
        job_id=str(job_starter.output),
    )
    # [END howto_sensor_emr_container]

    delete_eks_cluster = EksDeleteClusterOperator(
        task_id="delete_eks_cluster",
        cluster_name=eks_cluster_name,
        force_delete_compute=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    await_delete_eks_cluster = EksClusterStateSensor(
        task_id="await_delete_eks_cluster",
        cluster_name=eks_cluster_name,
        target_state=ClusterStates.NONEXISTENT,
        trigger_rule=TriggerRule.ALL_DONE,
        poke_interval=10,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=s3_bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        upload_s3_file,
        create_launch_template(launch_template_name),
        create_cluster_and_nodegroup,
        await_create_nodegroup,
        run_eksctl_commands(eks_cluster_name, eks_namespace),
        update_trust_policy_execution_role(eks_cluster_name, eks_namespace, job_role_name),
        # TEST BODY
        create_emr_eks_cluster,
        job_starter,
        job_waiter,
        # TEST TEARDOWN
        delete_iam_oidc_identity_provider(eks_cluster_name),
        delete_virtual_cluster(str(create_emr_eks_cluster.output)),
        delete_eks_cluster,
        await_delete_eks_cluster,
        delete_launch_template(launch_template_name),
        delete_bucket,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
