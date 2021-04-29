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
from unittest import mock

from airflow.plugins_manager import AirflowPlugin, EntryPointSource
from tests.test_utils.mock_plugins import mock_plugin_manager


def test_should_list_plugins_on_page_with_details(admin_client, checker):
    resp = admin_client.get('/plugin')
    checker.check_content_in_response("test_plugin", resp)
    checker.check_content_in_response("Airflow Plugins", resp)
    checker.check_content_in_response("source", resp)
    checker.check_content_in_response("<em>$PLUGINS_FOLDER/</em>test_plugin.py", resp)


def test_should_list_entrypoint_plugins_on_page_with_details(admin_client, checker):
    mock_plugin = AirflowPlugin()
    mock_plugin.name = "test_plugin"
    mock_plugin.source = EntryPointSource(
        mock.Mock(), mock.Mock(version='1.0.0', metadata={'name': 'test-entrypoint-testpluginview'})
    )
    with mock_plugin_manager(plugins=[mock_plugin]):
        resp = admin_client.get('/plugin')

    checker.check_content_in_response("test_plugin", resp)
    checker.check_content_in_response("Airflow Plugins", resp)
    checker.check_content_in_response("source", resp)
    checker.check_content_in_response("<em>test-entrypoint-testpluginview==1.0.0:</em> <Mock id=", resp)


def test_endpoint_should_not_be_unauthenticated(app, checker):
    resp = app.test_client().get('/plugin', follow_redirects=True)
    checker.check_content_not_in_response("test_plugin", resp)
    checker.check_content_in_response("Sign In - Airflow", resp)
