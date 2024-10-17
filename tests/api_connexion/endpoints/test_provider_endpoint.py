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

from unittest import mock

import pytest

from airflow.providers_manager import ProviderInfo

from tests_common.test_utils.api_connexion_utils import create_user, delete_user

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

MOCK_PROVIDERS = {
    "apache-airflow-providers-amazon": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-amazon",
            "name": "Amazon",
            "description": "`Amazon Web Services (AWS) <https://aws.amazon.com/>`__.\n",
            "versions": ["1.0.0"],
        },
        "package",
    ),
    "apache-airflow-providers-apache-cassandra": ProviderInfo(
        "1.0.0",
        {
            "package-name": "apache-airflow-providers-apache-cassandra",
            "name": "Apache Cassandra",
            "description": "`Apache Cassandra <http://cassandra.apache.org/>`__.\n",
            "versions": ["1.0.0"],
        },
        "package",
    ),
}


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestBaseProviderEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app, cleanup_providers_manager) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore


class TestGetProviders(TestBaseProviderEndpoint):
    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value={},
    )
    def test_response_200_empty_list(self, mock_providers):
        response = self.client.get("/api/v1/providers", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {"providers": [], "total_entries": 0}

    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_response_200(self, mock_providers):
        response = self.client.get("/api/v1/providers", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "providers": [
                {
                    "description": "Amazon Web Services (AWS) https://aws.amazon.com/",
                    "package_name": "apache-airflow-providers-amazon",
                    "version": "1.0.0",
                },
                {
                    "description": "Apache Cassandra http://cassandra.apache.org/",
                    "package_name": "apache-airflow-providers-apache-cassandra",
                    "version": "1.0.0",
                },
            ],
            "total_entries": 2,
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/providers")
        assert response.status_code == 401

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/providers", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403
