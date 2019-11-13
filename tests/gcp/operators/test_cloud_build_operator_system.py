# -*- coding: utf-8 -*-
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
"""System tests for Google Cloud Build operators"""

import pytest

from tests.gcp.operators.test_cloud_build_system_helper import GCPCloudBuildTestHelper
from tests.gcp.utils.gcp_authenticator import GCP_CLOUD_BUILD_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

# TODO: check how to refactor DAG / test helper
test_helper = GCPCloudBuildTestHelper()


@pytest.fixture
def helper():
    test_helper.create_repository_and_bucket()
    yield
    test_helper.delete_bucket()
    test_helper.delete_docker_images()
    test_helper.delete_repo()


@GcpSystemTest.skip(GCP_CLOUD_BUILD_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag():
    """
    System tests for Google Cloud Build operators

    It use a real service.
    """
    with provide_gcp_context(GCP_CLOUD_BUILD_KEY):
        GcpSystemTest.run_dag("example_gcp_cloud_build", GCP_DAG_FOLDER)
