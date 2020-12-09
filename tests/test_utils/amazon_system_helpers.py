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

import os
from contextlib import contextmanager
from typing import List

import pytest

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils import db
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.logging_command_executor import get_executor
from tests.test_utils.system_tests_class import SystemTest

AWS_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "providers", "amazon", "aws", "example_dags")


@contextmanager
def provide_aws_context():
    """
    Authenticates the context to be able use aws resources.

    Falls back to awscli default authentication methods via `.aws`` folder.
    """
    # TODO: Implement more authentication methods
    yield


@contextmanager
def provide_aws_s3_bucket(name):
    AmazonSystemTest.create_aws_s3_bucket(name)
    yield
    AmazonSystemTest.delete_aws_s3_bucket(name)


@pytest.mark.system("amazon")
class AmazonSystemTest(SystemTest):
    @staticmethod
    def _region_name():
        return os.environ.get("REGION_NAME")

    @staticmethod
    def _registry_id():
        return os.environ.get("REGISTRY_ID")

    @staticmethod
    def _image():
        return os.environ.get("IMAGE")

    @staticmethod
    def _execution_role_arn():
        return os.environ.get("EXECUTION_ROLE_ARN")

    @staticmethod
    def _remove_resources():
        # remove all created/existing resources flag
        return os.environ.get("REMOVE_RESOURCES", False)

    @classmethod
    def execute_with_ctx(cls, cmd: List[str]):
        """
        Executes command with context created by provide_aws_context.
        """
        executor = get_executor()
        with provide_aws_context():
            executor.execute_cmd(cmd=cmd)

    @staticmethod
    def create_connection(aws_conn_id: str, region: str) -> None:
        """
        Create aws connection with region

        :param aws_conn_id: id of the aws connection to create
        :type aws_conn_id: str
        :param region: aws region name to use in extra field of the aws connection
        :type region: str
        """
        db.merge_conn(
            Connection(
                conn_id=aws_conn_id,
                conn_type="aws",
                extra=f'{{"region_name": "{region}"}}',
            ),
        )

    @classmethod
    def create_aws_s3_bucket(cls, name: str) -> None:
        """
        Creates the aws bucket with the given name.

        :param name: name of the bucket
        """
        cmd = ["aws", "s3api", "create-bucket", "--bucket", name]
        cls.execute_with_ctx(cmd)

    @classmethod
    def delete_aws_s3_bucket(cls, name: str) -> None:
        """
        Deletes the aws bucket with the given name. It needs to empty the bucket before it can be deleted.

        :param name: name of the bucket
        """
        cmd = ["aws", "s3", "rm", f"s3://{name}", "--recursive"]
        cls.execute_with_ctx(cmd)
        cmd = ["aws", "s3api", "delete-bucket", "--bucket", name]
        cls.execute_with_ctx(cmd)

    @classmethod
    def create_emr_default_roles(cls) -> None:
        """Create EMR Default roles for running system test

        This will create the default IAM roles:
        - `EMR_EC2_DefaultRole`
        - `EMR_DefaultRole`
        """
        cmd = ["aws", "emr", "create-default-roles"]
        cls.execute_with_ctx(cmd)

    @staticmethod
    def create_ecs_cluster(aws_conn_id: str, cluster_name: str) -> None:
        """
        Create ecs cluster with given name

        If specified cluster exists, it doesn't change and new cluster will not be created.

        :param aws_conn_id: id of the aws connection to use when creating boto3 client/resource
        :type aws_conn_id: str
        :param cluster_name: name of the cluster to create in aws ecs
        :type cluster_name: str
        """
        hook = AwsBaseHook(
            aws_conn_id=aws_conn_id,
            client_type="ecs",
        )
        hook.conn.create_cluster(
            clusterName=cluster_name,
            capacityProviders=[
                "FARGATE_SPOT",
                "FARGATE",
            ],
            defaultCapacityProviderStrategy=[
                {
                    "capacityProvider": "FARGATE_SPOT",
                    "weight": 1,
                    "base": 0,
                },
                {
                    "capacityProvider": "FARGATE",
                    "weight": 1,
                    "base": 0,
                },
            ],
        )

    @staticmethod
    def delete_ecs_cluster(aws_conn_id: str, cluster_name: str) -> None:
        """
        Delete ecs cluster with given short name or full Amazon Resource Name (ARN)

        :param aws_conn_id: id of the aws connection to use when creating boto3 client/resource
        :type aws_conn_id: str
        :param cluster_name: name of the cluster to delete in aws ecs
        :type cluster_name: str
        """
        hook = AwsBaseHook(
            aws_conn_id=aws_conn_id,
            client_type="ecs",
        )
        hook.conn.delete_cluster(
            cluster=cluster_name,
        )

    @staticmethod
    def create_ecs_task_definition(
        aws_conn_id: str,
        task_definition: str,
        container: str,
        image: str,
        execution_role_arn: str,
        awslogs_group: str,
        awslogs_region: str,
        awslogs_stream_prefix: str,
    ) -> None:
        """
        Create ecs task definition with given name

        :param aws_conn_id: id of the aws connection to use when creating boto3 client/resource
        :type aws_conn_id: str
        :param task_definition: family name for task definition to create in aws ecs
        :type task_definition: str
        :param container: name of the container
        :type container: str
        :param image: image used to start a container,
            format: `registry_id`.dkr.ecr.`region`.amazonaws.com/`repository_name`:`tag`
        :type image: str
        :param execution_role_arn: task execution role that the Amazon ECS container agent can assume,
            format: arn:aws:iam::`registry_id`:role/`role_name`
        :type execution_role_arn: str
        :param awslogs_group: awslogs group option in log configuration
        :type awslogs_group: str
        :param awslogs_region: awslogs region option in log configuration
        :type awslogs_region: str
        :param awslogs_stream_prefix: awslogs stream prefix option in log configuration
        :type awslogs_stream_prefix: str
        """
        hook = AwsBaseHook(
            aws_conn_id=aws_conn_id,
            client_type="ecs",
        )
        hook.conn.register_task_definition(
            family=task_definition,
            executionRoleArn=execution_role_arn,
            networkMode="awsvpc",
            containerDefinitions=[
                {
                    "name": container,
                    "image": image,
                    "cpu": 256,
                    "memory": 512,  # hard limit
                    "memoryReservation": 512,  # soft limit
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": awslogs_group,
                            "awslogs-region": awslogs_region,
                            "awslogs-stream-prefix": awslogs_stream_prefix,
                        },
                    },
                },
            ],
            requiresCompatibilities=[
                "FARGATE",
            ],
            cpu="256",  # task cpu limit (total of all containers)
            memory="512",  # task memory limit (total of all containers)
        )

    @staticmethod
    def delete_ecs_task_definition(aws_conn_id: str, task_definition: str) -> None:
        """
        Delete all revisions of given ecs task definition

        :param aws_conn_id: id of the aws connection to use when creating boto3 client/resource
        :type aws_conn_id: str
        :param task_definition: family prefix for task definition to delete in aws ecs
        :type task_definition: str
        """
        hook = AwsBaseHook(
            aws_conn_id=aws_conn_id,
            client_type="ecs",
        )
        response = hook.conn.list_task_definitions(
            familyPrefix=task_definition,
            status="ACTIVE",
            sort="ASC",
            maxResults=100,
        )
        revisions = [arn.split(":")[-1] for arn in response["taskDefinitionArns"]]
        for revision in revisions:
            hook.conn.deregister_task_definition(
                taskDefinition=f"{task_definition}:{revision}",
            )

    @staticmethod
    def is_ecs_task_definition_exists(aws_conn_id: str, task_definition: str) -> bool:
        """
        Check whether given task definition exits in ecs

        :param aws_conn_id: id of the aws connection to use when creating boto3 client/resource
        :type aws_conn_id: str
        :param task_definition: family prefix for task definition to check in aws ecs
        :type task_definition: str
        """
        hook = AwsBaseHook(
            aws_conn_id=aws_conn_id,
            client_type="ecs",
        )
        response = hook.conn.list_task_definition_families(
            familyPrefix=task_definition,
            status="ACTIVE",
            maxResults=100,
        )
        return task_definition in response["families"]
