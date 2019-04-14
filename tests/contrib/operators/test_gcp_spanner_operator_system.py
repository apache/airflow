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

from tests.contrib.operators.test_gcp_spanner_operator_system_helper import \
    GCPSpannerTestHelper
from tests.contrib.utils.base_gcp_system_test_case import \
    SKIP_TEST_WARNING, DagGcpSystemTestCase
from tests.contrib.utils.gcp_authenticator import GCP_SPANNER_KEY


@unittest.skipIf(DagGcpSystemTestCase.skip_check(GCP_SPANNER_KEY), SKIP_TEST_WARNING)
class CloudSpannerExampleDagsTest(DagGcpSystemTestCase):
    def __init__(self, method_name='runTest'):
        super(CloudSpannerExampleDagsTest, self).__init__(
            method_name,
            dag_id='example_gcp_spanner',
            gcp_key=GCP_SPANNER_KEY)
        self.helper = GCPSpannerTestHelper()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        try:
            self.helper.delete_instance()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()
        super(CloudSpannerExampleDagsTest, self).tearDown()

    def test_run_example_dag_spanner(self):
        self._run_dag()
