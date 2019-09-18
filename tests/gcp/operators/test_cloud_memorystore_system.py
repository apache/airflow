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
"""System tests for Google Cloud Memorystore operators"""
import unittest

from tests.gcp.operators.test_cloud_memorystore_system_helper import GCPCloudMemorystoreTestHelper
from tests.contrib.utils.base_gcp_system_test_case import SKIP_TEST_WARNING, TestDagGcpSystem
from tests.contrib.utils.gcp_authenticator import GCP_MEMORYSTORE  # TODO: Update it


@unittest.skipIf(TestDagGcpSystem.skip_check(GCP_MEMORYSTORE), SKIP_TEST_WARNING)
class CloudBuildExampleDagsSystemTest(TestDagGcpSystem):
    """
    System tests for Google Cloud Memorystore operators

    It use a real service.
    """

    def __init__(self, method_name="runTest"):
        super().__init__(
            method_name,
            dag_id="gcp_cloud_memorystore",
            dag_name="example_cloud_memorystore.py",
            require_local_executor=False,
            gcp_key=GCP_MEMORYSTORE,
        )
        self.helper = GCPCloudMemorystoreTestHelper()

    def setUp(self):
        super().setUp()
        self.gcp_authenticator.gcp_authenticate()
        try:
            self.helper.create_bucket()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()

    def test_run_example_dag(self):
        self._run_dag()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        try:
            self.helper.delete_bucket()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()
        super().tearDown()
