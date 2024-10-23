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

TEST_OPERATION = {"name": "test-operation", "metadata": {}, "done": False}

TEST_INSTANCE = {
    "name": "test-instance",
    "createTime": "2014-10-02T15:01:23Z",
    "updateTime": "2014-10-02T15:01:23Z",
    "labels": {},
    "state": "ACTIVE",
    "kmsKey": "projects/test-project/locations/us-central1/keyRings/my-kr/cryptoKeys/my-kms-key",
}


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
            self.financial_services_hook = FinancialServicesHook()

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
        instances.get.return_value.execute.return_value = TEST_INSTANCE

        response = self.financial_services_hook.get_instance(
            instance_resource_uri="projects/test-project/locations/us-central1/instances/unit-test-instance"
        )

        instances.get.assert_called_once_with(
            name="projects/test-project/locations/us-central1/instances/unit-test-instance"
        )

        assert response == TEST_INSTANCE

    @patch("airflow.providers.google.cloud.hooks.financial_services.FinancialServicesHook.get_conn")
    def test_create_instance(self, mock_get_conn):
        self.financial_services_hook.connection = mock_get_conn.return_value

        projects = self.financial_services_hook.connection.projects.return_value
        locations = projects.locations.return_value
        instances = locations.instances.return_value
        instances.create.return_value.execute.return_value = TEST_OPERATION

        response = self.financial_services_hook.create_instance(
            instance_id="test-instance",
            kms_key_uri="projects/test-project/locations/us-central1/keyRings/my-kr/cryptoKeys/my-kms-key",
            location_resource_uri="projects/test-project/locations/us-central1",
        )

        instances.create.assert_called_once_with(
            parent="projects/test-project/locations/us-central1",
            instanceId="test-instance",
            body={
                "kmsKey": "projects/test-project/locations/us-central1/keyRings/my-kr/cryptoKeys/my-kms-key"
            },
        )

        assert response == TEST_OPERATION

    @patch("airflow.providers.google.cloud.hooks.financial_services.FinancialServicesHook.get_conn")
    def test_delete_instance(self, mock_get_conn):
        self.financial_services_hook.connection = mock_get_conn.return_value

        projects = self.financial_services_hook.connection.projects.return_value
        locations = projects.locations.return_value
        instances = locations.instances.return_value
        instances.delete.return_value.execute.return_value = TEST_OPERATION

        response = self.financial_services_hook.delete_instance(
            instance_resource_uri="projects/test-project/locations/us-central1/instances/unit-test-instance"
        )

        instances.delete.assert_called_once_with(
            name="projects/test-project/locations/us-central1/instances/unit-test-instance"
        )

        assert response == TEST_OPERATION
