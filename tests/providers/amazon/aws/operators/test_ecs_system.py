#
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
#

from tests.test_utils.amazon_system_helpers import AWS_DAG_FOLDER, AmazonSystemTest


class ECSSystemTest(AmazonSystemTest):
    """
    ECS System Test to run and test example ECS dags

    Required variables.env file content (from your account):
        # Auto-export all variables
        set -a

        # aws parameters
        REGION_NAME="eu-west-1"
        REGISTRY_ID="123456789012"
        IMAGE="alpine:3.9"
        SUBNET_ID="subnet-068e9654a3c357a"
        SECURITY_GROUP_ID="sg-054dc69874a651"
        EXECUTION_ROLE_ARN="arn:aws:iam::123456789012:role/FooBarRole"

        # remove all created/existing resources flag
        # comment out to keep resources or use empty string
        # REMOVE_RESOURCES="True"
    """

    # should be same as in the example dag
    aws_conn_id = "aws_ecs"
    cluster = "c"
    task_definition = "hello-world"
    container = "hello-world-container"
    awslogs_group = "/ecs/hello-world"
    awslogs_stream_prefix = "prefix_b"  # only prefix without container name

    def setUp(self):
        super().setUp()
        self.create_connection(
            aws_conn_id=self.aws_conn_id,
            region=self._region_name(),
        )

        # create ecs cluster if it does not exist
        self.create_ecs_cluster(
            aws_conn_id=self.aws_conn_id,
            cluster_name=self.cluster,
        )

        # create task_definition if it does not exist
        task_definition_exists = self.is_ecs_task_definition_exists(
            aws_conn_id=self.aws_conn_id,
            task_definition=self.task_definition,
        )
        if not task_definition_exists:
            self.create_ecs_task_definition(
                aws_conn_id=self.aws_conn_id,
                task_definition=self.task_definition,
                container=self.container,
                image=self._image(),
                execution_role_arn=self._execution_role_arn(),
                awslogs_group=self.awslogs_group,
                awslogs_region=self._region_name(),
                awslogs_stream_prefix=self.awslogs_stream_prefix,
            )

    def tearDown(self):
        # remove all created/existing resources in tear down
        if self._remove_resources():
            self.delete_ecs_cluster(
                aws_conn_id=self.aws_conn_id,
                cluster_name=self.cluster,
            )
            self.delete_ecs_task_definition(
                aws_conn_id=self.aws_conn_id,
                task_definition=self.task_definition,
            )
        super().tearDown()

    def test_run_example_dag_ecs_fargate_dag(self):
        self.run_dag("ecs_fargate_dag", AWS_DAG_FOLDER)
