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

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.marketing_platform.example_dags.example_display_video import BUCKET
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY, GMP_KEY
from tests.test_utils.gcp_system_helpers import MARKETING_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

# Requires the following scope:
SCOPES = [
    "https://www.googleapis.com/auth/doubleclickbidmanager",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/display-video"
]


@pytest.mark.system("google.marketing_platform")
@pytest.mark.credential_file(GMP_KEY)
class DisplayVideoSystemTest(GoogleSystemTest):

    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(BUCKET)

    def tearDown(self):
        self.delete_gcs_bucket(BUCKET)
        with provide_gcp_context(GCP_BIGQUERY_KEY, scopes=SCOPES):
            hook = BigQueryHook()
            hook.delete_dataset(dataset_id='airflow_test', delete_contents=True)
            super().tearDown()

    @provide_gcp_context(GMP_KEY, scopes=SCOPES)
    def test_run_example_dag(self):
        self.run_dag('example_display_video', MARKETING_DAG_FOLDER)
