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


from tests.providers.google.cloud.operators.test_video_intelligence_system_helper import (
    GCPVideoIntelligenceHelper,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_AI_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest


@skip_gcp_system(GCP_AI_KEY, require_local_executor=True)
class CloudVideoIntelligenceExampleDagsTest(SystemTest):
    helper = GCPVideoIntelligenceHelper()

    @provide_gcp_context(GCP_AI_KEY)
    def setUp(self):
        self.helper.create_bucket()
        super().setUp()

    @provide_gcp_context(GCP_AI_KEY)
    def tearDown(self):
        self.helper.delete_bucket()
        super().tearDown()

    @provide_gcp_context(GCP_AI_KEY)
    def test_run_example_dag_spanner(self):
        self.run_dag('example_gcp_video_intelligence', CLOUD_DAG_FOLDER)
