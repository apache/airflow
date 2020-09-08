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
import os
from itertools import product

import pytest

from airflow.providers.google.cloud.example_dags.example_gcs_to_sftp import (
    BUCKET_SRC,
    OBJECT_SRC_1,
    OBJECT_SRC_2,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.credential_file(GCP_GCS_KEY)
class GcsToSftpExampleDagsSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()

        # 1. Create buckets
        self.create_gcs_bucket(BUCKET_SRC)

        # 2. Prepare files
        for bucket_src, object_source in product(
            (
                BUCKET_SRC,
                "{}/subdir-1".format(BUCKET_SRC),
                "{}/subdir-2".format(BUCKET_SRC),
            ),
            (OBJECT_SRC_1, OBJECT_SRC_2),
        ):
            source_path = "gs://{}/{}".format(bucket_src, object_source)
            self.upload_content_to_gcs(
                lines=f"{os.urandom(1 * 1024 * 1024)}", bucket=source_path, filename=object_source
            )

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag(self):
        self.run_dag("example_gcs_to_sftp", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        self.delete_gcs_bucket(BUCKET_SRC)
        super().tearDown()
