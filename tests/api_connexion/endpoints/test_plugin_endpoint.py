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

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_plugins import mock_plugin_manager


class TestGetPlugins(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(
            cls.app,  # type: ignore
            username="test",
            role_name="Test",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
                (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore

    def test_get_plugins_return_200(self):
        mock_plugin = AirflowPlugin()
        mock_plugin.name = "test_plugin"
        with mock_plugin_manager(plugins=[mock_plugin]):
            response = self.client.get("api/v1/plugins", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json == {
            'plugins': [
                {
                    'attrs': {
                        'admin_views': [],
                        'appbuilder_menu_items': [],
                        'appbuilder_views': [],
                        'executors': [],
                        'flask_blueprints': [],
                        'global_operator_extra_links': [],
                        'hooks': [],
                        'macros': [],
                        'menu_links': [],
                        'operator_extra_links': [],
                        'source': None,
                    },
                    'name': 'test_plugin',
                    'number': 1,
                }
            ],
            'total_entries': 1,
        }

    def test_get_plugins_works_with_more_plugins(self):
        mock_plugin = AirflowPlugin()
        mock_plugin.name = "test_plugin"
        mock_plugin_2 = AirflowPlugin()
        mock_plugin_2.name = "test_plugin2"
        with mock_plugin_manager(plugins=[mock_plugin, mock_plugin_2]):
            response = self.client.get("api/v1/plugins", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 2

    def test_get_plugins_return_404_if_no_plugins(self):
        with mock_plugin_manager(plugins=[]):
            response = self.client.get("api/v1/plugins", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert response.json == {
            'detail': "Plugin not found",
            'status': 404,
            'title': 'Not Found',
            'type': EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/plugins")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/plugins", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403
