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
import json
import os
from unittest import mock

import httpx
import pytest
import tenacity

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook


def get_airflow_connection(unused_conn_id=None):
    return Connection(conn_id='http_default', conn_type='http', host='test:8080/', extra='{"bareer": "test"}')


def get_airflow_connection_with_port(unused_conn_id=None):
    return Connection(conn_id='http_default', conn_type='http', host='test.com', port=1234)


@pytest.fixture
def setup_hook():
    yield HttpHook(method='GET')


@pytest.fixture
def setup_lowercase_hook():
    yield HttpHook(method='get')


@pytest.fixture
def setup_post_hook():
    yield HttpHook(method='POST')


class TestHttpHook:
    """Test get, post and raise_for_status"""

    def test_raise_for_status_with_200(self, httpx_mock, setup_hook):
        httpx_mock.add_response(
            url='http://test:8080/v1/test', status_code=200, data='{"status":{"status": 200}}'
        )
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = setup_hook.run('v1/test')
            assert resp.text == '{"status":{"status": 200}}'

    @mock.patch('httpx.Client')
    @mock.patch('httpx.Request')
    def test_get_request_with_port(self, request_mock, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            expected_url = 'http://test.com:1234/some/endpoint'
            for endpoint in ['some/endpoint', '/some/endpoint']:

                setup_hook.run(endpoint)

                request_mock.assert_called_once_with(
                    mock.ANY, expected_url, headers=mock.ANY, params=mock.ANY
                )

                request_mock.reset_mock()

    def test_get_request_do_not_raise_for_status_if_check_response_is_false(self, httpx_mock, setup_hook):
        httpx_mock.add_response(
            url='http://test:8080/v1/test',
            status_code=404,
            data='{"status":{"status": 404}}',
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = setup_hook.run('v1/test', extra_options={'check_response': False})
            assert resp.text == '{"status":{"status": 404}}'

    def test_hook_contains_header_from_extra_field(self, setup_hook):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            expected_conn = get_airflow_connection()
            conn = setup_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) == conn.headers
            assert conn.headers.get('bareer') == 'test'

    def test_hook_with_method_in_lowercase(self, httpx_mock, setup_lowercase_hook):
        data = "test_params=aaaa"
        httpx_mock.add_response(
            url='http://test.com:1234/v1/test?test_params=aaaa',
            status_code=200,
            data='{"status":{"status": 200}}',
        )
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            try:
                setup_lowercase_hook.run('v1/test', data=data)
            except ConnectionError:
                pass

    def test_hook_uses_provided_header(self, setup_hook):
        conn = setup_hook.get_conn(headers={"bareer": "newT0k3n"})
        assert conn.headers.get('bareer') == "newT0k3n"

    def test_hook_has_no_header_from_extra(self, setup_hook):
        conn = setup_hook.get_conn()
        assert conn.headers.get('bareer') is None

    def test_hooks_header_from_extra_is_overridden(self, setup_hook):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            conn = setup_hook.get_conn()
            assert conn.headers.get('bareer') == 'test'

    def test_hooks_header_from_extra_is_overridden_and_used(self, httpx_mock, setup_hook):
        httpx_mock.add_response(
            url='http://test:8080/v1/test',
            status_code=200,
            data='{"status":{"status": 200}}',
            match_headers={"bareer": "test"},
        )
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            setup_hook.run('v1/test', extra_options={'check_response': False})

    def test_post_request(self, httpx_mock, setup_post_hook):
        httpx_mock.add_response(
            method='POST', url='http://test:8080/v1/test', status_code=200, data='{"status":{"status": 200}}'
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = setup_post_hook.run('v1/test')
            assert resp.status_code == 200

    def test_post_request_with_error_code(self, httpx_mock, setup_post_hook):
        httpx_mock.add_response(
            method='POST',
            url='http://test:8080/v1/test',
            status_code=418,
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            with pytest.raises(AirflowException):
                setup_post_hook.run('v1/test')

    def test_post_request_do_not_raise_for_status_if_check_response_is_false(
        self, httpx_mock, setup_post_hook
    ):
        httpx_mock.add_response(
            method='POST',
            url='http://test:8080/v1/test',
            status_code=418,
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = setup_post_hook.run('v1/test', extra_options={'check_response': False})
            assert resp.status_code == 418

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_retry_on_conn_error(self, mocked_client, setup_hook):

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(7),
            retry=tenacity.retry_if_exception_type(httpx.NetworkError),
        )

        def send_and_raise(unused_request, **kwargs):
            raise httpx.NetworkError(message="ConnectionError")

        mocked_client().send.side_effect = send_and_raise
        # The job failed for some reason
        with pytest.raises(tenacity.RetryError):
            setup_hook.run_with_advanced_retry(endpoint='v1/test', _retry_args=retry_args)
        assert setup_hook._retry_obj.stop.max_attempt_number + 1 == mocked_client.call_count

    def test_run_with_advanced_retry(self, httpx_mock, setup_hook):

        httpx_mock.add_response(url='http://test:8080/v1/test', status_code=200)

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception_type(Exception),
            reraise=True,
        )
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            response = setup_hook.run_with_advanced_retry(endpoint='v1/test', _retry_args=retry_args)
            assert isinstance(response, httpx.Response)

    def test_header_from_extra_and_run_method_are_merged(self, httpx_mock, setup_hook):
        httpx_mock.add_response(
            url='http://test:8080/v1/test',
            status_code=200,
            match_headers={"bareer": "test", "some_other_header": "test"},
        )
        # The job failed for some reason
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            setup_hook.run('v1/test', headers={'some_other_header': 'test'})

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_http_connection(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http', host='localhost', schema='http')
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'http://localhost'

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_https_connection(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http', host='localhost', schema='https')
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'https://localhost'

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_host_encoded_http_connection(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http', host='http://localhost')
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'http://localhost'

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_host_encoded_https_connection(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http', host='https://localhost')
        mock_get_connection.return_value = conn
        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'https://localhost'

    def test_method_converted_to_uppercase_when_created_in_lowercase(self, setup_lowercase_hook):
        assert setup_lowercase_hook.method == 'GET'

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_connection_without_host(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http')
        mock_get_connection.return_value = conn

        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'http://'

    def test_json_request_get(self, httpx_mock):
        obj1 = {'a': 1, 'b': 'abc', 'c': [1, 2, {"d": 10}]}

        httpx_mock.add_response(
            method='GET', url='http://test:8080/v1/test', match_content=json.dumps(obj1).encode('utf-8')
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            # will raise NoMockAddress exception if obj1 != request.json()
            HttpHook(method='GET').run('v1/test', json=obj1)

    def test_json_request_post(self, httpx_mock):
        obj1 = {'a': 1, 'b': 'abc', 'c': [1, 2, {"d": 10}]}

        httpx_mock.add_response(
            method='POST', url='http://test:8080/v1/test', match_content=json.dumps(obj1).encode('utf-8')
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            # will raise NoMockAddress exception if obj1 != request.json()
            HttpHook(method='POST').run('v1/test', json=obj1)

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_verify_set_to_true_by_default(self, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            setup_hook.run('/some/endpoint')
            mock_client.assert_called_once_with(verify=True, cert=None, proxies=None)

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_verify_can_be_overridden(self, mock_client, setup_hook):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            setup_hook.run('/some/endpoint', extra_options={'verify': False})
            mock_client.assert_called_once_with(verify=False, cert=None, proxies=None)

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_cert_can_be_overridden(self, mock_client, setup_hook):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            setup_hook.run('/some/endpoint', extra_options={'cert': '/tmp/private.crt'})
            mock_client.assert_called_once_with(verify=True, cert='/tmp/private.crt', proxies=None)

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_proxies_can_be_overridden(self, mock_client, setup_hook):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            setup_hook.run('/some/endpoint', extra_options={'proxies': {"http://localhost": 'http://proxy'}})
            mock_client.assert_called_once_with(
                verify=True, cert=None, proxies={"http://localhost": 'http://proxy'}
            )

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    def test_verifu_parameter_set(self, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):

            setup_hook.run('/some/endpoint', extra_options={'verify': '/tmp/overridden.crt'})
            mock_client.assert_called_once_with(cert=None, proxies=None, verify='/tmp/overridden.crt')

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_requests_ca_bundle_env_var(self, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):

            setup_hook.run('/some/endpoint')
            mock_client.assert_called_once_with(cert=None, proxies=None, verify='/tmp/test.crt')

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_respects_requests_ca_bundle_env_var(self, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):

            setup_hook.run('/some/endpoint', extra_options={'verify': True})
            mock_client.assert_called_once_with(cert=None, proxies=None, verify='/tmp/test.crt')

    @mock.patch('airflow.providers.http.hooks.http.httpx.Client')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_false_parameter_overwrites_set_requests_ca_bundle_env_var(self, mock_client, setup_hook):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            setup_hook.run('/some/endpoint', extra_options={'verify': False})
            mock_client.assert_called_once_with(cert=None, proxies=None, verify=False)
