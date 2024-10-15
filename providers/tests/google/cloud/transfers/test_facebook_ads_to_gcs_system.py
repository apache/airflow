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

import json
import os
from contextlib import contextmanager

import pytest
from tests_common.test_utils.gcp_system_helpers import (
    GoogleSystemTest,
    provide_gcp_context,
)
from tests_common.test_utils.system_tests import get_test_run

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.google.cloud.example_dags import example_facebook_ads_to_gcs
from airflow.utils.process_utils import patch_environ

from providers.tests.google.cloud.utils.gcp_authenticator import GCP_BIGQUERY_KEY

CREDENTIALS_DIR = os.environ.get("CREDENTIALS_DIR", "/files/airflow-breeze-config/keys")
FACEBOOK_KEY = "facebook.json"
FACEBOOK_CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, FACEBOOK_KEY)
CONNECTION_TYPE = os.environ.get("CONNECTION_TYPE", "facebook_social")
FACEBOOK_CONNECTION_ID = os.environ.get("FACEBOOK_CONNECTION_ID", "facebook_default")
CONFIG_REQUIRED_FIELDS = ["app_id", "app_secret", "access_token", "account_id"]


@contextmanager
def provide_facebook_connection(key_file_path: str):
    """
    Context manager that provides a temporary value of AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT
    connection. It build a new connection that includes path to provided service json,
    required scopes and project id.

    :param key_file_path: Path to file with FACEBOOK credentials .json file.
    """
    if not key_file_path.endswith(".json"):
        raise AirflowException("Use a JSON key file.")
    with open(key_file_path) as credentials:
        creds = json.load(credentials)
    missing_keys = CONFIG_REQUIRED_FIELDS - creds.keys()
    if missing_keys:
        message = f"{missing_keys} fields are missing"
        raise AirflowException(message)
    conn = Connection(conn_id=FACEBOOK_CONNECTION_ID, conn_type=CONNECTION_TYPE, extra=json.dumps(creds))
    with patch_environ({f"AIRFLOW_CONN_{conn.conn_id.upper()}": conn.get_uri()}):
        yield


@pytest.mark.credential_file(FACEBOOK_KEY)
@pytest.mark.credential_file(GCP_BIGQUERY_KEY)
@pytest.mark.system("google.cloud")
class TestFacebookAdsToGcsExampleDagsSystem(GoogleSystemTest):
    @provide_gcp_context(GCP_BIGQUERY_KEY)
    @provide_facebook_connection(FACEBOOK_CREDENTIALS_PATH)
    def test_dag_example(self):
        run = get_test_run(example_facebook_ads_to_gcs.dag)
        run()
