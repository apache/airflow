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

from airflow.providers.google.cloud.example_dags.example_video_intelligence import GCP_BUCKET_NAME
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_AI_KEY, GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCP_VIDEO_SOURCE_URL = "https://www.sample-videos.com/video123/mp4/720/big_buck_bunny_720p_1mb.mp4"


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_AI_KEY)
class CloudVideoIntelligenceExampleDagsTest(GoogleSystemTest):
    @provide_gcp_context(GCP_AI_KEY)
    def setUp(self):
        self.create_gcs_bucket(GCP_BUCKET_NAME, location="europe-north1")
        self.execute_with_ctx(
            cmd=["bash", "-c", f"curl {GCP_VIDEO_SOURCE_URL} | gsutil cp - gs://{GCP_BUCKET_NAME}/video.mp4"],
            key=GCP_GCS_KEY,
        )
        super().setUp()

    @provide_gcp_context(GCP_AI_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(GCP_BUCKET_NAME)
        super().tearDown()

    @provide_gcp_context(GCP_AI_KEY)
    def test_example_dag(self):
        self.run_dag('example_gcp_video_intelligence', CLOUD_DAG_FOLDER)
