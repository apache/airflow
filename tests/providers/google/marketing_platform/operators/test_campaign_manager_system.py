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
import pytest

from tests.providers.google.cloud.utils.gcp_authenticator import GOOGLE_CAMPAIGN_MANAGER_KEY
from tests.providers.google.marketing_platform.operators.test_campaign_manager_system_helper import (
    GoogleCampaignManagerTestHelper,
)
from tests.test_utils.gcp_system_helpers import MARKETING_DAG_FOLDER, provide_gcp_context
from tests.test_utils.system_tests_class import SystemTest

# Required scopes
SCOPES = [
    'https://www.googleapis.com/auth/dfatrafficking',
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/ddmconversions'
]


@pytest.mark.system("google.marketing_platform")
@pytest.mark.credential_file(GOOGLE_CAMPAIGN_MANAGER_KEY)
class CampaignManagerSystemTest(SystemTest):
    helper = GoogleCampaignManagerTestHelper()

    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY)
    def setUp(self):
        super().setUp()
        self.helper.create_bucket()

    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY)
    def tearDown(self):
        self.helper.delete_bucket()
        super().tearDown()

    @provide_gcp_context(GOOGLE_CAMPAIGN_MANAGER_KEY, scopes=SCOPES)
    def test_run_example_dag(self):
        self.run_dag('example_campaign_manager', MARKETING_DAG_FOLDER)
