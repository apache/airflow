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

from airflow.providers.google.cloud.example_dags.example_s3_to_gcs import UPLOAD_FILE
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

FILENAME = UPLOAD_FILE.split('/')[-1]


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_KEY)
class S3ToGCSSystemTest(GoogleSystemTest):
    """System test for S3 to GCS transfer operator.

    This test requires the following environment variables:

    GCP_PROJECT_ID=your-gcp-project-id
    GCP_GCS_BUCKET=unique-bucket-name-to-create
    S3BUCKET_NAME=unique-s3-bucket-name
    AWS_ACCESS_KEY_ID=your-aws-access-key
    AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key
    """

    def setUp(self) -> None:
        super().setUp()
        self.create_dummy_file(FILENAME)

    def tearDown(self) -> None:
        self.delete_dummy_file(FILENAME, dir_path='/tmp')
        super().tearDown()

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag_s3_to_gcs(self):
        self.run_dag('example_s3_to_gcs', CLOUD_DAG_FOLDER)
