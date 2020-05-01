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
from urllib.parse import urlparse

import pytest

from airflow.providers.google.cloud.example_dags.example_mlengine import (
    JOB_DIR, PREDICTION_OUTPUT, SAVED_MODEL_PATH, SUMMARY_STAGING, SUMMARY_TMP,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_AI_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

BUCKETS = {
    urlparse(bucket_url).netloc
    for bucket_url in {SAVED_MODEL_PATH, JOB_DIR, PREDICTION_OUTPUT, SUMMARY_TMP, SUMMARY_STAGING}
}


@pytest.mark.credential_file(GCP_AI_KEY)
class MlEngineExampleDagTest(GoogleSystemTest):

    @provide_gcp_context(GCP_AI_KEY)
    def setUp(self):
        super().setUp()
        for bucket in BUCKETS:
            self.create_gcs_bucket(bucket)

    @provide_gcp_context(GCP_AI_KEY)
    def tearDown(self):
        for bucket in BUCKETS:
            self.delete_gcs_bucket(bucket)
        super().tearDown()

    @provide_gcp_context(GCP_AI_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_gcp_mlengine', CLOUD_DAG_FOLDER)
