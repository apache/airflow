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
import unittest
from collections import OrderedDict
from http import HTTPStatus
from unittest import mock

import pytest
import requests
import requests_mock
import tenacity
from parameterized import parameterized
from requests.adapters import Response

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook


def get_airflow_connection(unused_conn_id=None):
    return Connection(conn_id='http_default', conn_type='http', host='test:8080/', extra='{"bareer": "test"}')


def get_airflow_connection_with_port(unused_conn_id=None):
    return Connection(conn_id='http_default', conn_type='http', host='test.com', port=1234)


class TestHttpHook(unittest.TestCase):
    """Test get, post and raise_for_status"""

    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        self.get_hook = HttpHook(method='GET')
        self.get_lowercase_hook = HttpHook(method='get')
        self.post_hook = HttpHook(method='POST')

    @requests_mock.mock()
    def test_raise_for_status_with_200(self, m):

        m.get('http://test:8080/v1/test', status_code=200, text='{"status":{"status": 200}}', reason='OK')
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = self.get_hook.run('v1/test')
            assert resp.text == '{"status":{"status": 200}}'

    @requests_mock.mock()
    @mock.patch('requests.Session')
    @mock.patch('requests.Request')
    def test_get_request_with_port(self, mock_requests, request_mock, mock_session):
        from requests.exceptions import MissingSchema

        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            expected_url = 'http://test.com:1234/some/endpoint'
            for endpoint in ['some/endpoint', '/some/endpoint']:

                try:
                    self.get_hook.run(endpoint)
                except MissingSchema:
                    pass

                request_mock.assert_called_once_with(
                    mock.ANY, expected_url, headers=mock.ANY, params=mock.ANY
                )

                request_mock.reset_mock()

    @requests_mock.mock()
    def test_get_request_do_not_raise_for_status_if_check_response_is_false(self, m):

        m.get(
            'http://test:8080/v1/test',
            status_code=404,
            text='{"status":{"status": 404}}',
            reason='Bad request',
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = self.get_hook.run('v1/test', extra_options={'check_response': False})
            assert resp.text == '{"status":{"status": 404}}'

    @requests_mock.mock()
    def test_hook_contains_header_from_extra_field(self, mock_requests):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            expected_conn = get_airflow_connection()
            conn = self.get_hook.get_conn()
            assert dict(conn.headers, **json.loads(expected_conn.extra)) == conn.headers
            assert conn.headers.get('bareer') == 'test'

    @requests_mock.mock()
    @mock.patch('requests.Request')
    def test_hook_with_method_in_lowercase(self, mock_requests, request_mock):
        from requests.exceptions import InvalidURL, MissingSchema

        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            data = "test params"
            try:
                self.get_lowercase_hook.run('v1/test', data=data)
            except (MissingSchema, InvalidURL):
                pass
            request_mock.assert_called_once_with(mock.ANY, mock.ANY, headers=mock.ANY, params=data)

    @requests_mock.mock()
    def test_hook_uses_provided_header(self, mock_requests):
        conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
        assert conn.headers.get('bareer') == "newT0k3n"

    @requests_mock.mock()
    def test_hook_has_no_header_from_extra(self, mock_requests):
        conn = self.get_hook.get_conn()
        assert conn.headers.get('bareer') is None

    @requests_mock.mock()
    def test_hooks_header_from_extra_is_overridden(self, mock_requests):
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
            assert conn.headers.get('bareer') == 'newT0k3n'

    @requests_mock.mock()
    def test_post_request(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test', status_code=200, text='{"status":{"status": 200}}', reason='OK'
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = self.post_hook.run('v1/test')
            assert resp.status_code == 200

    @requests_mock.mock()
    def test_post_request_with_error_code(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test',
            status_code=418,
            text='{"status":{"status": 418}}',
            reason='I\'m a teapot',
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            with pytest.raises(AirflowException):
                self.post_hook.run('v1/test')

    @requests_mock.mock()
    def test_post_request_do_not_raise_for_status_if_check_response_is_false(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test',
            status_code=418,
            text='{"status":{"status": 418}}',
            reason='I\'m a teapot',
        )

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            resp = self.post_hook.run('v1/test', extra_options={'check_response': False})
            assert resp.status_code == 418

    @mock.patch('airflow.providers.http.hooks.http.requests.Session')
    def test_retry_on_conn_error(self, mocked_session):

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(7),
            retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
        )

        def send_and_raise(unused_request, **kwargs):
            raise requests.exceptions.ConnectionError

        mocked_session().send.side_effect = send_and_raise
        # The job failed for some reason
        with pytest.raises(tenacity.RetryError):
            self.get_hook.run_with_advanced_retry(endpoint='v1/test', _retry_args=retry_args)
        assert self.get_hook._retry_obj.stop.max_attempt_number + 1 == mocked_session.call_count

    @requests_mock.mock()
    def test_run_with_advanced_retry(self, m):

        m.get('http://test:8080/v1/test', status_code=200, reason='OK')

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception_type(Exception),
            reraise=True,
        )
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            response = self.get_hook.run_with_advanced_retry(endpoint='v1/test', _retry_args=retry_args)
            assert isinstance(response, requests.Response)

    def test_header_from_extra_and_run_method_are_merged(self):
        def run_and_return(unused_session, prepped_request, unused_extra_options, **kwargs):
            return prepped_request

        # The job failed for some reason
        with mock.patch(
            'airflow.providers.http.hooks.http.HttpHook.run_and_check', side_effect=run_and_return
        ):
            with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
                prepared_request = self.get_hook.run('v1/test', headers={'some_other_header': 'test'})
                actual = dict(prepared_request.headers)
                assert actual.get('bareer') == 'test'
                assert actual.get('some_other_header') == 'test'

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

    def test_method_converted_to_uppercase_when_created_in_lowercase(self):
        assert self.get_lowercase_hook.method == 'GET'

    @mock.patch('airflow.providers.http.hooks.http.HttpHook.get_connection')
    def test_connection_without_host(self, mock_get_connection):
        conn = Connection(conn_id='http_default', conn_type='http')
        mock_get_connection.return_value = conn

        hook = HttpHook()
        hook.get_conn({})
        assert hook.base_url == 'http://'

    @parameterized.expand(
        [
            'GET',
            'POST',
        ]
    )
    @requests_mock.mock()
    def test_json_request(self, method, mock_requests):
        obj1 = {'a': 1, 'b': 'abc', 'c': [1, 2, {"d": 10}]}

        def match_obj1(request):
            return request.json() == obj1

        mock_requests.request(method=method, url='//test:8080/v1/test', additional_matcher=match_obj1)

        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            # will raise NoMockAddress exception if obj1 != request.json()
            HttpHook(method=method).run('v1/test', json=obj1)

    @mock.patch('airflow.providers.http.hooks.http.requests.Session.send')
    def test_verify_set_to_true_by_default(self, mock_session_send):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            self.get_hook.run('/some/endpoint')
            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies=OrderedDict(),
                stream=False,
                timeout=None,
                verify=True,
            )

    @mock.patch('airflow.providers.http.hooks.http.requests.Session.send')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_requests_ca_bundle_env_var(self, mock_session_send):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):

            self.get_hook.run('/some/endpoint')

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies=OrderedDict(),
                stream=False,
                timeout=None,
                verify='/tmp/test.crt',
            )

    @mock.patch('airflow.providers.http.hooks.http.requests.Session.send')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_respects_requests_ca_bundle_env_var(self, mock_session_send):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):

            self.get_hook.run('/some/endpoint', extra_options={'verify': True})

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies=OrderedDict(),
                stream=False,
                timeout=None,
                verify='/tmp/test.crt',
            )

    @mock.patch('airflow.providers.http.hooks.http.requests.Session.send')
    @mock.patch.dict(os.environ, {"REQUESTS_CA_BUNDLE": "/tmp/test.crt"})
    def test_verify_false_parameter_overwrites_set_requests_ca_bundle_env_var(self, mock_session_send):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ):
            self.get_hook.run('/some/endpoint', extra_options={'verify': False})

            mock_session_send.assert_called_once_with(
                mock.ANY,
                allow_redirects=True,
                cert=None,
                proxies=OrderedDict(),
                stream=False,
                timeout=None,
                verify=False,
            )

    @requests_mock.mock()
    def test_connection_success(self, m):
        m.get('http://test:8080', status_code=200, json={"status": {"status": 200}}, reason='OK')
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            status, msg = self.get_hook.test_connection()
            assert status is True
            assert msg == 'Connection successfully tested'

    @requests_mock.mock()
    def test_connection_failure(self, m):
        m.get('http://test:8080', status_code=500, json={"message": "internal server error"}, reason='NOT_OK')
        with mock.patch('airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection):
            status, msg = self.get_hook.test_connection()
            assert status is False
            assert msg == '500:NOT_OK'


class TestKeepAlive:
    def test_keep_alive_enabled(self):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ), mock.patch(
            'requests_toolbelt.adapters.socket_options.TCPKeepAliveAdapter.send'
        ) as tcp_keep_alive_send, mock.patch(
            'requests.adapters.HTTPAdapter.send'
        ) as http_send:
            hook = HttpHook(method='GET')
            response = Response()
            response.status_code = HTTPStatus.OK
            tcp_keep_alive_send.return_value = response
            http_send.return_value = response
            hook.run('v1/test')
            tcp_keep_alive_send.assert_called()
            http_send.assert_not_called()

    def test_keep_alive_disabled(self):
        with mock.patch(
            'airflow.hooks.base.BaseHook.get_connection', side_effect=get_airflow_connection_with_port
        ), mock.patch(
            'requests_toolbelt.adapters.socket_options.TCPKeepAliveAdapter.send'
        ) as tcp_keep_alive_send, mock.patch(
            'requests.adapters.HTTPAdapter.send'
        ) as http_send:
            hook = HttpHook(method='GET', tcp_keep_alive=False)
            response = Response()
            response.status_code = HTTPStatus.OK
            tcp_keep_alive_send.return_value = response
            http_send.return_value = response
            hook.run('v1/test')
            tcp_keep_alive_send.assert_not_called()
            http_send.assert_called()


send_email_test = mock.Mock()
