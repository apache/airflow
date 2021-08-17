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

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context
from tests.test_utils.salesforce_system_helpers import provide_salesforce_connection

CREDENTIALS_DIR = os.environ.get('CREDENTIALS_DIR', '/files/airflow-breeze-config/keys')
SALESFORCE_KEY = 'salesforce.json'
SALESFORCE_CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, SALESFORCE_KEY)


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_BIGQUERY_KEY)
@pytest.mark.credential_file(SALESFORCE_KEY)
@pytest.mark.system("google.cloud")
@pytest.mark.system("salesforce")
class TestSalesforceIntoGCSExample(GoogleSystemTest):
    @provide_gcp_context(GCP_BIGQUERY_KEY)
    @provide_salesforce_connection(SALESFORCE_CREDENTIALS_PATH)
    def test_run_example_dag_salesforce_to_gcs_operator(self):
        self.run_dag('example_salesforce_to_gcs', CLOUD_DAG_FOLDER)
