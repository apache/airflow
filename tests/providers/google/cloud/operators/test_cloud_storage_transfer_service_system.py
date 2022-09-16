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
from __future__ import annotations

import pytest

from airflow.providers.google.cloud.example_dags.example_cloud_storage_transfer_service_gcp import (
    GCP_PROJECT_ID,
    GCP_TRANSFER_FIRST_TARGET_BUCKET,
    GCP_TRANSFER_SECOND_TARGET_BUCKET,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_TRANSFER_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.fixture
def helper():
    with provide_gcp_context(GCP_GCS_TRANSFER_KEY):
        # Create buckets
        GoogleSystemTest.create_gcs_bucket(GCP_TRANSFER_SECOND_TARGET_BUCKET, location="asia-east1")
        GoogleSystemTest.create_gcs_bucket(GCP_TRANSFER_FIRST_TARGET_BUCKET)
        GoogleSystemTest.upload_content_to_gcs("test_contents", GCP_TRANSFER_FIRST_TARGET_BUCKET, "test.txt")

        # Grant bucket permissions
        project_number = GoogleSystemTest.get_project_number(GCP_PROJECT_ID)
        account_email = f"project-{project_number}@storage-transfer-service.iam.gserviceaccount.com"
        GoogleSystemTest.grant_bucket_access(GCP_TRANSFER_FIRST_TARGET_BUCKET, account_email)
        GoogleSystemTest.grant_bucket_access(GCP_TRANSFER_SECOND_TARGET_BUCKET, account_email)

        yield

        # Remove buckets
        GoogleSystemTest.delete_gcs_bucket(GCP_TRANSFER_SECOND_TARGET_BUCKET)
        GoogleSystemTest.delete_gcs_bucket(GCP_TRANSFER_FIRST_TARGET_BUCKET)


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_TRANSFER_KEY)
class GcpTransferExampleDagsSystemTest(GoogleSystemTest):
    def setUp(self):
        super().setUp()

    @pytest.mark.usefixtures("helper")
    @provide_gcp_context(GCP_GCS_TRANSFER_KEY)
    def test_run_example_dag_compute(self):
        self.run_dag('example_gcp_transfer', CLOUD_DAG_FOLDER)

    def tearDown(self):
        super().tearDown()
