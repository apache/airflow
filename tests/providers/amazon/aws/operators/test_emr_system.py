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

import pytest

from tests.test_utils.aws_system_helpers import AWS_DAG_FOLDER
from tests.test_utils.system_tests_class import SystemTest
from tests.utils.logging_command_executor import get_executor


@pytest.fixture
def create_emr_default_roles():
    """Create EMR Default roles for running system test

    This will create the default IAM roles:
    - `EMR_EC2_DefaultRole`
    - `EMR_DefaultRole`
    """
    executor = get_executor()
    executor.execute_cmd(["aws", "emr", "create-default-roles"])


@pytest.mark.system("amazon")
@pytest.mark.usefixtures("create_emr_default_roles")
class TestSystemAwsEmr(SystemTest):
    """
    System tests for AWS EMR operators
    """
    def test_run_example_dag_emr_automatic_steps(self):
        self.run_dag('emr_job_flow_automatic_steps_dag', AWS_DAG_FOLDER)

    def test_run_example_dag_emr_manual_steps(self):
        self.run_dag('emr_job_flow_manual_steps_dag', AWS_DAG_FOLDER)
