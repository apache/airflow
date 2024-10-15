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

import random
import string
import subprocess
from unittest import mock

import pytest
from tests_common.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context

from providers.tests.google.cloud.utils.gcp_authenticator import GCP_SECRET_MANAGER_KEY

BACKEND_IMPORT_PATH = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"


@pytest.mark.credential_file(GCP_SECRET_MANAGER_KEY)
class TestCloudSecretManagerBackendVariableSystem(GoogleSystemTest):
    def setup_method(self) -> None:
        self.unique_suffix = "".join(random.choices(string.ascii_lowercase, k=10))
        self.name = f"airflow-system-test-{self.unique_suffix}"
        self.secret_name = f"airflow-variables-{self.name}"

    @provide_gcp_context(GCP_SECRET_MANAGER_KEY, project_id=GoogleSystemTest._project_id())
    @mock.patch.dict("os.environ", AIRFLOW__SECRETS__BACKEND=BACKEND_IMPORT_PATH)
    def test_should_read_secret_from_variable(self):
        cmd = f'echo -n "TEST_CONTENT" | gcloud secrets create \
            {self.secret_name} --data-file=-  --replication-policy=automatic'
        subprocess.run(["bash", "-c", cmd], check=True)
        result = subprocess.check_output(["airflow", "variables", "get", self.name])
        assert "TEST_CONTENT" in result.decode()

    @provide_gcp_context(GCP_SECRET_MANAGER_KEY, project_id=GoogleSystemTest._project_id())
    def teardown_method(self) -> None:
        subprocess.run(["gcloud", "secrets", "delete", self.secret_name, "--quiet"], check=False)


@pytest.mark.credential_file(GCP_SECRET_MANAGER_KEY)
class TestCloudSecretManagerBackendConnectionSystem(GoogleSystemTest):
    def setup_method(self) -> None:
        self.unique_suffix = "".join(random.choices(string.ascii_lowercase, k=10))
        self.name = f"airflow-system-test-{self.unique_suffix}"
        self.secret_name = f"airflow-connections-{self.name}"

    @provide_gcp_context(GCP_SECRET_MANAGER_KEY, project_id=GoogleSystemTest._project_id())
    @mock.patch.dict("os.environ", AIRFLOW__SECRETS__BACKEND=BACKEND_IMPORT_PATH)
    def test_should_read_secret_from_variable(self):
        cmd = f'echo -n "mysql://user:pass@example.org" | gcloud secrets create \
            {self.secret_name} --data-file=- --replication-policy=automatic'
        subprocess.run(["bash", "-c", cmd], check=True)
        result = subprocess.check_output(["airflow", "connections", "get", self.name, "--output", "json"])
        assert "mysql://user:pass@example.org" in result.decode()
        assert self.name in result.decode()

    @provide_gcp_context(GCP_SECRET_MANAGER_KEY, project_id=GoogleSystemTest._project_id())
    def teardown_method(self) -> None:
        subprocess.run(["gcloud", "secrets", "delete", self.secret_name, "--quiet"], check=False)
