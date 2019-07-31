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

# pylint: disable=C0111
"""
This module contains various unit tests for
example_gcp_dlp DAG
"""

import unittest

from tests.contrib.utils.base_gcp_system_test_case import \
    SKIP_TEST_WARNING, DagGcpSystemTestCase
from tests.contrib.utils.gcp_authenticator import GCP_DLP_KEY


@unittest.skipIf(
    DagGcpSystemTestCase.skip_check(GCP_DLP_KEY), SKIP_TEST_WARNING)
class GcpDLPExampleDagsSystemTest(DagGcpSystemTestCase):
    def __init__(self, method_name='runTest'):
        super(GcpDLPExampleDagsSystemTest, self).__init__(
            method_name,
            dag_id='example_gcp_dlp',
            gcp_key=GCP_DLP_KEY)

    def test_run_example_dag_function(self):
        self._run_dag()
