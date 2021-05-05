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

import pytest

from airflow.plugins_manager import AirflowPlugin, EntryPointSource
from airflow.www.views import get_safe_url, truncate_task_duration
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_plugins import mock_plugin_manager
from tests.test_utils.www import check_content_in_response, check_content_not_in_response


def test_configuration_do_not_expose_config(admin_client):
    with conf_vars({('webserver', 'expose_config'): 'False'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    check_content_in_response(
        [
            'Airflow Configuration',
            '# Your Airflow administrator chose not to expose the configuration, '
            'most likely for security reasons.',
        ],
        resp,
    )


def test_configuration_expose_config(admin_client):
    with conf_vars({('webserver', 'expose_config'): 'True'}):
        resp = admin_client.get('configuration', follow_redirects=True)
    check_content_in_response(['Airflow Configuration', 'Running Configuration'], resp)


def test_redoc_should_render_template(capture_templates, admin_client):
    with capture_templates() as templates:
        resp = admin_client.get('redoc')
        check_content_in_response('Redoc', resp)

    assert len(templates) == 1
    assert templates[0].name == 'airflow/redoc.html'
    assert templates[0].local_context == {'openapi_spec_url': '/api/v1/openapi.yaml'}


def test_plugin_should_list_on_page_with_details(admin_client):
    resp = admin_client.get('/plugin')
    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>$PLUGINS_FOLDER/</em>test_plugin.py", resp)


def test_plugin_should_list_entrypoint_on_page_with_details(admin_client):
    mock_plugin = AirflowPlugin()
    mock_plugin.name = "test_plugin"
    mock_plugin.source = EntryPointSource(
        mock.Mock(), mock.Mock(version='1.0.0', metadata={'name': 'test-entrypoint-testpluginview'})
    )
    with mock_plugin_manager(plugins=[mock_plugin]):
        resp = admin_client.get('/plugin')

    check_content_in_response("test_plugin", resp)
    check_content_in_response("Airflow Plugins", resp)
    check_content_in_response("source", resp)
    check_content_in_response("<em>test-entrypoint-testpluginview==1.0.0:</em> <Mock id=", resp)


def test_plugin_endpoint_should_not_be_unauthenticated(app):
    resp = app.test_client().get('/plugin', follow_redirects=True)
    check_content_not_in_response("test_plugin", resp)
    check_content_in_response("Sign In - Airflow", resp)


@pytest.mark.parametrize(
    "url, content",
    [
        (
            "/taskinstance/list/?_flt_0_execution_date=2018-10-09+22:44:31",
            "List Task Instance",
        ),
        (
            "/taskreschedule/list/?_flt_0_execution_date=2018-10-09+22:44:31",
            "List Task Reschedule",
        ),
    ],
    ids=["instance", "reschedule"],
)
def test_task_start_date_filter(admin_client, url, content):
    resp = admin_client.get(url)
    # We aren't checking the logic of the date filter itself (that is built
    # in to FAB) but simply that our UTC conversion was run - i.e. it
    # doesn't blow up!
    check_content_in_response(content, resp)


@pytest.mark.parametrize(
    "test_url, expected_url",
    [
        ("", "/home"),
        ("http://google.com", "/home"),
        ("36539'%3balert(1)%2f%2f166", "/home"),
        (
            "http://localhost:8080/trigger?dag_id=test&origin=36539%27%3balert(1)%2f%2f166&abc=2",
            "/home",
        ),
        (
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%test_dag';alert(33)//",
            "/home",
        ),
        (
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%3Dtest_dag",
            "http://localhost:8080/trigger?dag_id=test_dag&origin=%2Ftree%3Fdag_id%3Dtest_dag",
        ),
    ],
)
@mock.patch("airflow.www.views.url_for")
def test_get_safe_url(mock_url_for, app, test_url, expected_url):
    mock_url_for.return_value = "/home"
    with app.test_request_context(base_url="http://localhost:8080"):
        assert get_safe_url(test_url) == expected_url


@pytest.mark.parametrize(
    "test_duration, expected_duration",
    [
        (0.12345, 0.123),
        (0.12355, 0.124),
        (3.12, 3.12),
        (9.99999, 10.0),
        (10.01232, 10),
    ],
)
def test_truncate_task_duration(test_duration, expected_duration):
    assert truncate_task_duration(test_duration) == expected_duration
