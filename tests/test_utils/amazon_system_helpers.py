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

from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.system_tests_class import SystemTest
from tests.utils.logging_command_executor import get_executor

AWS_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "amazon", "aws", "example_dags"
)


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

    @classmethod
    def execute_with_ctx(cls, cmd: List[str]):
        """
        Executes command with context created by provide_aws_context.
        """
        executor = get_executor()
        with provide_aws_context():
            executor.execute_cmd(cmd=cmd)

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
