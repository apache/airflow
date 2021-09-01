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

from airflow.providers.google.marketing_platform.example_dags.example_analytics import BUCKET, BUCKET_FILENAME
from tests.providers.google.cloud.utils.gcp_authenticator import GMP_KEY
from tests.test_utils.gcp_system_helpers import MARKETING_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

DATA = """pagePath,dimension1
how_to_make_pizza,1
how_to_make_doughnuts,2
how_to_build_a_treehouse,3
how_to_make_paper_airplanes,4
"""


# Required scopes
SCOPES = [
    'https://www.googleapis.com/auth/analytics',
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/cloud-platform',
]


@pytest.fixture
def helper():
    GoogleSystemTest.create_gcs_bucket(BUCKET)
    GoogleSystemTest.upload_content_to_gcs(DATA, BUCKET, BUCKET_FILENAME)
    yield
    GoogleSystemTest.delete_gcs_bucket(BUCKET)


@pytest.mark.system("google.marketing_platform")
@pytest.mark.credential_file(GMP_KEY)
class TestSystemGoogleAds(GoogleSystemTest):
    def setUp(self):
        super().setUp()

    @pytest.mark.usefixtures("helper")
    @provide_gcp_context(GMP_KEY, scopes=SCOPES)
    def test_run_example_dag(self):
        self.run_dag('example_google_analytics', MARKETING_DAG_FOLDER)

    def tearDown(self):
        super().tearDown()
