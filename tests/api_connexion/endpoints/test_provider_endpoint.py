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

import unittest
from collections import OrderedDict
from unittest import mock

from airflow.security import permissions
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.config import conf_vars

MOCK_PROVIDERS = OrderedDict(
    [
        (
            'apache-airflow-providers-amazon',
            (
                '1.0.0',
                {
                    'package-name': 'apache-airflow-providers-amazon',
                    'name': 'Amazon',
                    'description': '`Amazon Web Services (AWS) <https://aws.amazon.com/>`__.\n',
                    'versions': ['1.0.0'],
                },
            ),
        ),
        (
            'apache-airflow-providers-apache-cassandra',
            (
                '1.0.0',
                {
                    'package-name': 'apache-airflow-providers-apache-cassandra',
                    'name': 'Apache Cassandra',
                    'description': '`Apache Cassandra <http://cassandra.apache.org/>`__.\n',
                    'versions': ['1.0.0'],
                },
            ),
        ),
    ]
)


class TestBaseProviderEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(
            cls.app,  # type:ignore
            username="test",
            role_name="Test",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_PROVIDER)],  # type: ignore
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

        cls.client = None

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore


class TestGetProviders(TestBaseProviderEndpoint):
    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value={},
    )
    def test_response_200_empty_list(self, mock_providers):
        response = self.client.get("/api/v1/providers", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        self.assertEqual({"providers": [], "total_entries": 0}, response.json)

    @mock.patch(
        "airflow.providers_manager.ProvidersManager.providers",
        new_callable=mock.PropertyMock,
        return_value=MOCK_PROVIDERS,
    )
    def test_response_200(self, mock_providers):
        response = self.client.get("/api/v1/providers", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        self.assertEqual(
            {
                'providers': [
                    {
                        'description': 'Amazon Web Services (AWS) https://aws.amazon.com/',
                        'package_name': 'apache-airflow-providers-amazon',
                        'version': '1.0.0',
                    },
                    {
                        'description': 'Apache Cassandra http://cassandra.apache.org/',
                        'package_name': 'apache-airflow-providers-apache-cassandra',
                        'version': '1.0.0',
                    },
                ],
                'total_entries': 2,
            },
            response.json,
        )

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/providers")
        assert response.status_code == 401

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/providers", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403
