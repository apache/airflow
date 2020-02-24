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
import os

import pytest

from tests.providers.google.cloud.utils.gcp_authenticator import GOOGLE_CAMPAIGN_MANAGER_KEY
from tests.test_utils.gcp_system_helpers import MARKETING_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

# Required scopes
SCOPES = [
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/ddmconversions'
]

BUCKET = os.environ.get("MARKETING_BUCKET", "test-cm-bucket")


@pytest.mark.system("google.marketing_platform")
@pytest.mark.credential_file(GOOGLE_CAMPAIGN_MANAGER_KEY)
class CampaignManagerSystemTest(GoogleSystemTest):
    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(BUCKET)

    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(BUCKET)
        super().tearDown()

    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY, scopes=SCOPES)
    def test_run_example_dag(self):
        self.run_dag('example_campaign_manager', MARKETING_DAG_FOLDER)
