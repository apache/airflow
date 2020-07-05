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

import pytest

from airflow.providers.google.suite.example_dags.example_gcs_to_sheets import BUCKET
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import GSUITE_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

# Required scopes
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
]


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_KEY)
class GoogleSheetsToGCSExampleDagsSystemTest(GoogleSystemTest):

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(BUCKET)

    @provide_gcp_context(GCP_GCS_KEY, scopes=SCOPES)
    def test_run_example_dag_function(self):
        self.run_dag('example_gcs_to_sheets', GSUITE_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(BUCKET)
        super().tearDown()
