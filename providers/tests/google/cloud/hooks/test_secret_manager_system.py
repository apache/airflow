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

import os

import pytest

from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook

from dev.tests_common.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context
from providers.tests.google.cloud.utils.gcp_authenticator import GCP_SECRET_MANAGER_KEY

TEST_SECRET_ID = os.environ.get("GCP_SECRET_MANAGER_SECRET_ID", "test-secret")
TEST_SECRET_VALUE = os.environ.get("GCP_SECRET_MANAGER_SECRET_VALUE", "test-secret-value")
TEST_SECRET_VALUE_UPDATED = os.environ.get("GCP_SECRET_MANAGER_VALUE_UPDATED", "test-secret-value-updated")
TEST_MISSING_SECRET_ID = os.environ.get("GCP_SECRET_MANAGER_MISSING_SECRET_ID", "test-missing-secret")


@pytest.fixture
def helper_one_version():
    GoogleSystemTest.delete_secret(TEST_SECRET_ID, silent=True)
    GoogleSystemTest.create_secret(TEST_SECRET_ID, TEST_SECRET_VALUE)
    yield
    GoogleSystemTest.delete_secret(TEST_SECRET_ID)


@pytest.fixture
def helper_two_versions():
    GoogleSystemTest.delete_secret(TEST_SECRET_ID, silent=True)
    GoogleSystemTest.create_secret(TEST_SECRET_ID, TEST_SECRET_VALUE)
    GoogleSystemTest.update_secret(TEST_SECRET_ID, TEST_SECRET_VALUE_UPDATED)
    yield
    GoogleSystemTest.delete_secret(TEST_SECRET_ID)


@pytest.mark.system("google.secret_manager")
@pytest.mark.credential_file(GCP_SECRET_MANAGER_KEY)
class TestSecretsManagerSystem(GoogleSystemTest):
    @pytest.mark.usefixtures("helper_one_version")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_secret_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_SECRET_ID)
        assert TEST_SECRET_VALUE == secret

    @pytest.mark.usefixtures("helper_one_version")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_missing_secret_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_MISSING_SECRET_ID)
        assert secret is None

    @pytest.mark.usefixtures("helper_two_versions")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_secret_different_versions_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_SECRET_ID)
        assert TEST_SECRET_VALUE_UPDATED == secret
        secret = hook.get_secret(secret_id=TEST_SECRET_ID, secret_version="1")
        assert TEST_SECRET_VALUE == secret
        secret = hook.get_secret(secret_id=TEST_SECRET_ID, secret_version="2")
        assert TEST_SECRET_VALUE_UPDATED == secret
