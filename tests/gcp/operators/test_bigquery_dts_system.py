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

from tests.gcp.operators.test_bigquery_dts_system_helper import (
    GcpBigqueryDtsTestHelper,
)
from tests.contrib.utils.base_gcp_system_test_case import (
    SKIP_TEST_WARNING,
    TestDagGcpSystem,
)
from tests.contrib.utils.gcp_authenticator import GCP_BIGQUERY_KEY

from airflow.gcp.example_dags.example_bigquery_dts import (
    GCP_PROJECT_ID,
    GCP_DTS_BQ_DATASET,
    GCP_DTS_BQ_TABLE,
    BUCKET_URI
)


@unittest.skipIf(TestDagGcpSystem.skip_check(GCP_BIGQUERY_KEY), SKIP_TEST_WARNING)
class GcpBigqueryDtsSystemTest(TestDagGcpSystem):
    def __init__(self, method_name="runTest"):
        super(GcpBigqueryDtsSystemTest, self).__init__(
            method_name, dag_id="example_gcp_bigquery_dts", gcp_key=GCP_BIGQUERY_KEY
        )
        self.helper = GcpBigqueryDtsTestHelper()

    def setUp(self):
        super().setUp()
        self.gcp_authenticator.gcp_authenticate()
        self.helper.create_dataset(
            project_id=GCP_PROJECT_ID,
            dataset=GCP_DTS_BQ_DATASET,
            table=GCP_DTS_BQ_TABLE,
        )
        self.helper.upload_data(dataset=GCP_DTS_BQ_DATASET, table=GCP_DTS_BQ_TABLE, gcs_file=BUCKET_URI)
        self.gcp_authenticator.gcp_revoke_authentication()

    def tearDown(self):
        self.gcp_authenticator.gcp_authenticate()
        self.helper.delete_dataset(
            project_id=GCP_PROJECT_ID, dataset=GCP_DTS_BQ_DATASET
        )
        self.gcp_authenticator.gcp_revoke_authentication()
        super().tearDown()

    def test_run_example_dag_function(self):
        self._run_dag()
