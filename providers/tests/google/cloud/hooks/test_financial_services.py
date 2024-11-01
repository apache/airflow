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

from unittest.mock import patch

from airflow.providers.google.cloud.hooks.financial_services import FinancialServicesHook

PROJECT_ID = "test-project"
REGION = "us-central1"
KMS_KEY_RING = "test-key-ring"
KMS_KEY = "test-key"
INSTANCE_ID = "test-instance"


def mock_init(
    self,
    gcp_conn_id,
    impersonation_chain=None,
):
    pass


class TestFinancialServicesHook:
    def setup_method(self):
        with patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_init,
        ):
            self.financial_services_hook = FinancialServicesHook(discovery_doc={})

    @patch("airflow.providers.google.cloud.hooks.financial_services.build_from_document")
    def test_get_conn(self, mock_build):
        conn = self.financial_services_hook.get_conn()

        mock_build.assert_called_once()
        assert conn == mock_build.return_value
        assert conn == self.financial_services_hook.connection

    @patch("airflow.providers.google.cloud.hooks.financial_services.FinancialServicesHook.get_conn")
    def test_get_instance(self, mock_get_conn):
        self.financial_services_hook.connection = mock_get_conn.return_value

        projects = self.financial_services_hook.connection.projects.return_value
        locations = projects.locations.return_value
        instances = locations.instances.return_value

        self.financial_services_hook.get_instance(
            project_id=PROJECT_ID, region=REGION, instance_id=INSTANCE_ID
        )
        instances.get.assert_called_once_with(
            name=f"projects/{PROJECT_ID}/locations/{REGION}/instances/{INSTANCE_ID}"
        )

    @patch("airflow.providers.google.cloud.hooks.financial_services.FinancialServicesHook.get_conn")
    def test_create_instance(self, mock_get_conn):
        self.financial_services_hook.connection = mock_get_conn.return_value

        projects = self.financial_services_hook.connection.projects.return_value
        locations = projects.locations.return_value
        instances = locations.instances.return_value

        self.financial_services_hook.create_instance(
            project_id=PROJECT_ID,
            region=REGION,
            instance_id=INSTANCE_ID,
            kms_key_ring_id=KMS_KEY_RING,
            kms_key_id=KMS_KEY,
        )

        instances.create.assert_called_once_with(
            parent=f"projects/{PROJECT_ID}/locations/{REGION}",
            instanceId=INSTANCE_ID,
            body={
                "kmsKey": f"projects/{PROJECT_ID}/locations/{REGION}/keyRings/{KMS_KEY_RING}/cryptoKeys/{KMS_KEY}"
            },
        )

    @patch("airflow.providers.google.cloud.hooks.financial_services.FinancialServicesHook.get_conn")
    def test_delete_instance(self, mock_get_conn):
        self.financial_services_hook.connection = mock_get_conn.return_value

        projects = self.financial_services_hook.connection.projects.return_value
        locations = projects.locations.return_value
        instances = locations.instances.return_value

        self.financial_services_hook.delete_instance(
            project_id=PROJECT_ID, region=REGION, instance_id=INSTANCE_ID
        )

        instances.delete.assert_called_once_with(
            name=f"projects/{PROJECT_ID}/locations/{REGION}/instances/{INSTANCE_ID}"
        )
