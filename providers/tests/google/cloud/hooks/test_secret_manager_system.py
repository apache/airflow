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

from tests_common.test_utils.gcp_system_helpers import GoogleSystemTest

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
