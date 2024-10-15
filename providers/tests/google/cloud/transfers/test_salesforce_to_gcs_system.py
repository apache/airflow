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

import os

import pytest
from tests_common.test_utils.gcp_system_helpers import (
    GoogleSystemTest,
    provide_gcp_context,
)
from tests_common.test_utils.salesforce_system_helpers import provide_salesforce_connection
from tests_common.test_utils.system_tests import get_test_run

from airflow.providers.google.cloud.example_dags import example_salesforce_to_gcs

from providers.tests.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY

CREDENTIALS_DIR = os.environ.get("CREDENTIALS_DIR", "/files/airflow-breeze-config/keys")
SALESFORCE_KEY = "salesforce.json"
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
        run = get_test_run(example_salesforce_to_gcs.dag)
        run()
