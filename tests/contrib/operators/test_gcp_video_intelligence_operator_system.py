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
import unittest

from tests.contrib.operators.test_gcp_video_intelligence_operator_system_helper import (
    GCPVideoIntelligenceHelper,
)
from tests.contrib.utils.base_gcp_system_test_case import SKIP_TEST_WARNING, DagGcpSystemTestCase
from tests.contrib.utils.gcp_authenticator import GCP_AI_KEY


@unittest.skipIf(DagGcpSystemTestCase.skip_check(GCP_AI_KEY), SKIP_TEST_WARNING)
class CloudVideoIntelligenceExampleDagsTest(DagGcpSystemTestCase):
    def __init__(self, method_name="runTest"):
        super(CloudVideoIntelligenceExampleDagsTest, self).__init__(
            method_name, dag_id="example_gcp_video_intelligence", gcp_key=GCP_AI_KEY
        )
        self.helper = GCPVideoIntelligenceHelper()

    def setUp(self):
        self.gcp_authenticator.gcp_authenticate()
        try:
            self.helper.create_bucket()
            self.gcp_authenticator.gcp_revoke_authentication()
        finally:
            pass
        super(CloudVideoIntelligenceExampleDagsTest, self).setUp()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        try:
            self.helper.delete_bucket()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()
        super(CloudVideoIntelligenceExampleDagsTest, self).tearDown()

    def test_run_example_dag_spanner(self):
        self._run_dag()
