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
from __future__ import annotations

import pytest

from airflow.providers.google.cloud.example_dags.example_compute_ssh import (
    GCE_INSTANCE,
    GCE_ZONE,
    GCP_PROJECT_ID,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_COMPUTE_SSH_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_COMPUTE_SSH_KEY)
class GcpComputeSSHExampleDagsSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_COMPUTE_SSH_KEY)
    def setUp(self) -> None:
        super().setUp()
        self.create_target_instance()

    @provide_gcp_context(GCP_COMPUTE_SSH_KEY)
    def tearDown(self) -> None:
        self.delete_target_instance()
        super().tearDown()

    @provide_gcp_context(GCP_COMPUTE_SSH_KEY)
    def test_run_example_dag_compute_engine_ssh(self):
        self.run_dag('example_compute_ssh', CLOUD_DAG_FOLDER)

    def delete_target_instance(self):
        self.execute_cmd(
            [
                'gcloud',
                'compute',
                'instances',
                'delete',
                GCE_INSTANCE,
                '--project',
                GCP_PROJECT_ID,
                '--quiet',
                '--verbosity=none',
                '--zone',
                GCE_ZONE,
            ]
        )

    def create_target_instance(self):
        self.execute_cmd(
            [
                'gcloud',
                'compute',
                'instances',
                'create',
                GCE_INSTANCE,
                '--project',
                GCP_PROJECT_ID,
                '--zone',
                GCE_ZONE,
                "--metadata=enable-oslogin=TRUE",
            ]
        )
