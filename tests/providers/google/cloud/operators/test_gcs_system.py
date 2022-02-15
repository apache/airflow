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

from tests.providers.google.cloud.operators.test_gcs_system_helper import GcsSystemTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.fixture(scope="module")
def helper():
    return GcsSystemTestHelper()


@pytest.fixture
def file_to_upload(helper):
    helper.create_file_to_upload()
    yield
    helper.remove_file_to_upload()


@pytest.fixture
def script_to_transform(helper):
    helper.create_script_to_transform()
    yield
    helper.remove_script_to_transform()


@pytest.fixture
def saved_file(helper):
    # file is created by operator inside DAG
    yield
    helper.remove_saved_file()


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_KEY)
class GoogleCloudStorageExampleDagsTest(GoogleSystemTest):
    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        super().tearDown()

    @provide_gcp_context(GCP_GCS_KEY)
    @pytest.mark.usefixtures("file_to_upload", "script_to_transform", "saved_file")
    def test_run_example_dag(self):
        self.run_dag('example_gcs', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_KEY)
    @pytest.mark.usefixtures("file_to_upload")
    def test_run_example_gcs_sensor_dag(self):
        self.run_dag('example_gcs_sensors', CLOUD_DAG_FOLDER)
