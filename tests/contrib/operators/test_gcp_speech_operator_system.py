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

from tests.contrib.utils.base_gcp_system_test_case import SKIP_TEST_WARNING, DagGcpSystemTestCase
from tests.contrib.utils.gcp_authenticator import GCP_GCS_KEY

from tests.contrib.operators.test_gcp_speech_operator_system_helper import GCPTextToSpeechTestHelper


@unittest.skipIf(DagGcpSystemTestCase.skip_check(GCP_GCS_KEY), SKIP_TEST_WARNING)
class GCPTextToSpeechExampleDagSystemTest(DagGcpSystemTestCase):
    def setUp(self):
        super(GCPTextToSpeechExampleDagSystemTest, self).setUp()
        self.gcp_authenticator.gcp_authenticate()
        self.helper.create_target_bucket()
        self.gcp_authenticator.gcp_revoke_authentication()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        self.helper.delete_target_bucket()
        self.gcp_authenticator.gcp_revoke_authentication()
        super(GCPTextToSpeechExampleDagSystemTest, self).tearDown()

    def __init__(self, method_name="runTest"):
        super(GCPTextToSpeechExampleDagSystemTest, self).__init__(
            method_name, dag_id="example_gcp_speech", gcp_key=GCP_GCS_KEY
        )
        self.helper = GCPTextToSpeechTestHelper()

    def test_run_example_dag_gcp_text_to_speech(self):
        self._run_dag()
