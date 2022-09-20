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
"""System tests for Google Cloud Composer operators"""
from __future__ import annotations

import pytest

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUD_COMPOSER
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_CLOUD_COMPOSER)
class CloudComposerSystemTest(GoogleSystemTest):
    """
    System tests for Google Cloud Composer operators

    It use a real service.
    """

    @provide_gcp_context(GCP_CLOUD_COMPOSER)
    def setUp(self):
        super().setUp()

    @provide_gcp_context(GCP_CLOUD_COMPOSER)
    def test_run_example_dag_composer(self):
        self.run_dag('composer_dag1', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_CLOUD_COMPOSER)
    def tearDown(self):
        super().tearDown()
