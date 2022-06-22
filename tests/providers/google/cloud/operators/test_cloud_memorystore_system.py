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
"""System tests for Google Cloud Memorystore operators"""
import pytest

from airflow.providers.google.cloud.example_dags.example_cloud_memorystore import BUCKET_NAME
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_MEMORYSTORE
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_MEMORYSTORE)
class CloudMemorystoreSystemTest(GoogleSystemTest):
    """
    System tests for Google Cloud Memorystore operators

    It use a real service.
    """

    @provide_gcp_context(GCP_MEMORYSTORE)
    def setUp(self):
        super().setUp()
        self.create_gcs_bucket(BUCKET_NAME, location="europe-north1")

    @provide_gcp_context(GCP_MEMORYSTORE)
    def test_run_example_dag_memorystore_redis(self):
        self.run_dag('gcp_cloud_memorystore_redis', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_MEMORYSTORE)
    def test_run_example_dag_memorystore_memcached(self):
        self.run_dag('gcp_cloud_memorystore_memcached', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_MEMORYSTORE)
    def tearDown(self):
        self.delete_gcs_bucket(BUCKET_NAME)
        super().tearDown()
