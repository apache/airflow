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

from airflow.models import Connection
from airflow.providers.google.cloud.example_dags.example_azure_fileshare_to_gcs import (
    AZURE_DIRECTORY_NAME,
    AZURE_SHARE_NAME,
    DEST_GCS_BUCKET,
)
from airflow.utils.session import create_session
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.azure_system_helpers import AzureSystemTest, provide_azure_fileshare
from tests.test_utils.db import clear_db_connections
from tests.test_utils.gcp_system_helpers import (
    CLOUD_DAG_FOLDER,
    GoogleSystemTest,
    provide_gcp_context,
    provide_gcs_bucket,
)

AZURE_LOGIN = os.environ.get('AZURE_LOGIN', 'default_login')
AZURE_KEY = os.environ.get('AZURE_KEY', 'default_key')
CONN_ID = 'azure_fileshare_default'
AZURE_FILE_NAME = 'file.bin'


@pytest.fixture
def provide_azure_fileshare_with_directory():
    with create_session() as session:
        wasb_conn_id = Connection(
            conn_id=CONN_ID,
            conn_type='https',
            login=AZURE_LOGIN,
            password=AZURE_KEY,
        )
        session.add(wasb_conn_id)  # pylint: disable=expression-not-assigned

    with provide_azure_fileshare(
        share_name=AZURE_SHARE_NAME,
        wasb_conn_id=CONN_ID,
        file_name=AZURE_FILE_NAME,
        directory=AZURE_DIRECTORY_NAME,
    ):
        yield

    clear_db_connections()


@pytest.fixture
def provide_gcs_bucket_basic():
    with provide_gcs_bucket(bucket_name=DEST_GCS_BUCKET):
        yield


@pytest.mark.credential_file(GCP_GCS_KEY)
@pytest.mark.system("google.cloud")
class AzureFileShareToGCSOperatorExampleDAGsTest(GoogleSystemTest, AzureSystemTest):
    @pytest.mark.usefixtures('provide_gcs_bucket_basic', 'provide_azure_fileshare_with_directory')
    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag_azure_fileshare_to_gcs(self):
        self.run_dag('azure_fileshare_to_gcs_example', CLOUD_DAG_FOLDER)
