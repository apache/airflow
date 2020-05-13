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

from tests.test_utils.amazon_system_helpers import AWS_DAG_FOLDER, AmazonSystemTest


class EmrSystemTest(AmazonSystemTest):
    """
    System tests for AWS EMR operators
    """
    @classmethod
    def setup_class(cls):
        cls.create_emr_default_roles()

    def test_run_example_dag_emr_automatic_steps(self):
        self.run_dag('emr_job_flow_automatic_steps_dag', AWS_DAG_FOLDER)

    def test_run_example_dag_emr_manual_steps(self):
        self.run_dag('emr_job_flow_manual_steps_dag', AWS_DAG_FOLDER)
