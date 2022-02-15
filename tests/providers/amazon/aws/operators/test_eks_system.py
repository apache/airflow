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
import pytest

from tests.test_utils.amazon_system_helpers import (
    AWS_DAG_FOLDER,
    AWS_EKS_KEY,
    AmazonSystemTest,
    provide_aws_context,
)


@pytest.mark.system("amazon.aws")
@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(AWS_EKS_KEY)
class ExampleDagsSystemTest(AmazonSystemTest):
    @provide_aws_context(AWS_EKS_KEY)
    def setUp(self):
        super().setUp()

    @provide_aws_context(AWS_EKS_KEY)
    def tearDown(self):
        super().tearDown()

    @pytest.mark.long_running
    @provide_aws_context(AWS_EKS_KEY)
    def test_run_example_dag_eks_create_cluster(self):
        self.run_dag('create_eks_cluster_dag', AWS_DAG_FOLDER)

    @pytest.mark.long_running
    @provide_aws_context(AWS_EKS_KEY)
    def test_run_example_dag_eks_create_nodegroup(self):
        self.run_dag('create_eks_nodegroup_dag', AWS_DAG_FOLDER)

    @pytest.mark.long_running
    @provide_aws_context(AWS_EKS_KEY)
    def test_run_example_dag_create_eks_cluster_and_nodegroup(self):
        self.run_dag('create_eks_cluster_and_nodegroup_dag', AWS_DAG_FOLDER)

    @pytest.mark.long_running
    @provide_aws_context(AWS_EKS_KEY)
    def test_run_example_dag_eks_run_pod(self):
        self.run_dag('eks_run_pod_dag', AWS_DAG_FOLDER)
