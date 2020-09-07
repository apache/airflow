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
"""System tests for Google Cloud Build operators"""
import pytest

from airflow.providers.google.cloud.example_dags.example_gcs_to_gcs import (
    BUCKET_1_DST,
    BUCKET_1_SRC,
    BUCKET_2_DST,
    BUCKET_2_SRC,
    BUCKET_3_DST,
    BUCKET_3_SRC,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_KEY)
class GcsToGcsExampleDagsSystemTest(GoogleSystemTest):
    def create_buckets(self):
        """Create a buckets in Google Cloud Storage service with sample content."""

        # 1. Create bucket
        for name in [BUCKET_1_SRC, BUCKET_1_DST, BUCKET_2_SRC, BUCKET_2_DST, BUCKET_3_SRC, BUCKET_3_DST]:
            self.create_gcs_bucket(name)

        # 2. Prepare parents
        first_parent = "gs://{}/parent-1.bin".format(BUCKET_1_SRC)
        second_parent = "gs://{}/parent-2.bin".format(BUCKET_1_SRC)

        self.execute_with_ctx(
            [
                "bash",
                "-c",
                "cat /dev/urandom | head -c $((1 * 1024 * 1024)) | gsutil cp - {}".format(first_parent),
            ],
            key=GCP_GCS_KEY,
        )

        self.execute_with_ctx(
            [
                "bash",
                "-c",
                "cat /dev/urandom | head -c $((1 * 1024 * 1024)) | gsutil cp - {}".format(second_parent),
            ],
            key=GCP_GCS_KEY,
        )

        self.upload_to_gcs(first_parent, f"gs://{BUCKET_1_SRC}/file.bin")
        self.upload_to_gcs(first_parent, f"gs://{BUCKET_1_SRC}/subdir/file.bin")
        self.upload_to_gcs(first_parent, f"gs://{BUCKET_2_SRC}/file.bin")
        self.upload_to_gcs(first_parent, f"gs://{BUCKET_2_SRC}/subdir/file.bin")
        self.upload_to_gcs(second_parent, f"gs://{BUCKET_2_DST}/file.bin")
        self.upload_to_gcs(second_parent, f"gs://{BUCKET_2_DST}/subdir/file.bin")
        self.upload_to_gcs(second_parent, f"gs://{BUCKET_3_DST}/file.bin")
        self.upload_to_gcs(second_parent, f"gs://{BUCKET_3_DST}/subdir/file.bin")

        self.delete_gcs_bucket(first_parent)
        self.delete_gcs_bucket(second_parent)

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()
        self.create_buckets()

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_gcs_to_gcs', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        for name in [BUCKET_1_SRC, BUCKET_1_DST, BUCKET_2_SRC, BUCKET_2_DST, BUCKET_3_SRC, BUCKET_3_DST]:
            self.delete_gcs_bucket(name)
        super().tearDown()
