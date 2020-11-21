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

from unittest import TestCase

import pytest

from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_SECRET_MANAGER_KEY
from tests.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context

TEST_SECRET_ID = "test-secret"
TEST_SECRET_VALUE = "test-secret-value"
TEST_SECRET_VALUE_UPDATED = "test-secret-value-updated"
TEST_MISSING_SECRET_ID = "test-missing-secret"


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
class TestSystemSecretsManager(TestCase):
    @pytest.mark.usefixtures("helper_one_version")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_secret_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_SECRET_ID)
        self.assertEqual(TEST_SECRET_VALUE, secret)

    @pytest.mark.usefixtures("helper_one_version")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_missing_secret_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_MISSING_SECRET_ID)
        self.assertIsNone(secret)

    @pytest.mark.usefixtures("helper_two_versions")
    @provide_gcp_context(GCP_SECRET_MANAGER_KEY)
    def test_read_secret_different_versions_from_secret_manager(self):
        hook = SecretsManagerHook()
        secret = hook.get_secret(secret_id=TEST_SECRET_ID)
        self.assertEqual(TEST_SECRET_VALUE_UPDATED, secret)
        secret = hook.get_secret(secret_id=TEST_SECRET_ID, secret_version='1')
        self.assertEqual(TEST_SECRET_VALUE, secret)
        secret = hook.get_secret(secret_id=TEST_SECRET_ID, secret_version='2')
        self.assertEqual(TEST_SECRET_VALUE_UPDATED, secret)
