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

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_DISPLAY_VIDEO_KEY
from tests.providers.google.marketing_platform.operators.test_display_video_system_helper import (
    GcpDisplayVideoSystemTestHelper,
)
from tests.test_utils.gcp_system_helpers import provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

# Requires the following scope:
SCOPES = ["https://www.googleapis.com/auth/doubleclickbidmanager"]


@skip_gcp_system(GCP_DISPLAY_VIDEO_KEY)
class DisplayVideoSystemTest(SystemTest):
    helper = GcpDisplayVideoSystemTestHelper()

    @provide_gcp_context(GCP_DISPLAY_VIDEO_KEY)
    def setUp(self):
        super().setUp()
        self.helper.create_bucket()

    @provide_gcp_context(GCP_DISPLAY_VIDEO_KEY)
    def tearDown(self):
        self.helper.delete_bucket()
        super().tearDown()

    @provide_gcp_context(GCP_DISPLAY_VIDEO_KEY, scopes=SCOPES)
    def test_run_example_dag(self):
        self.run_dag('example_display_video', "airflow/providers/google/marketing_platform/example_dags")
