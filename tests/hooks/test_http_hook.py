# -*- coding: utf-8 -*-
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
import unittest

import requests
import requests_mock
import tenacity

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import Connection
from tests.compat import mock


def get_airflow_connection(unused_conn_id=None):
    return Connection(
        conn_id='http_default',
        conn_type='http',
        host='test:8080/',
        extra='{"bareer": "test"}'
    )


def get_airflow_connection_with_port(unused_conn_id=None):
    return Connection(
        conn_id='http_default',
        conn_type='http',
        host='test.com',
        port=1234
    )


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

        m.get(
            'http://test:8080/v1/test',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK'
        )
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            resp = self.get_hook.run('v1/test')
            self.assertEqual(resp.text, '{"status":{"status": 200}}')

    @requests_mock.mock()
    @mock.patch('requests.Session')
    @mock.patch('requests.Request')
    def test_get_request_with_port(self, mock_requests, request_mock, mock_session):
        from requests.exceptions import MissingSchema

        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection_with_port
        ):
            expected_url = 'http://test.com:1234/some/endpoint'
            for endpoint in ['some/endpoint', '/some/endpoint']:

                try:
                    self.get_hook.run(endpoint)
                except MissingSchema:
                    pass

                request_mock.assert_called_once_with(
                    mock.ANY,
                    expected_url,
                    headers=mock.ANY,
                    params=mock.ANY
                )

                request_mock.reset_mock()

    @requests_mock.mock()
    def test_get_request_do_not_raise_for_status_if_check_response_is_false(self, m):

        m.get(
            'http://test:8080/v1/test',
            status_code=404,
            text='{"status":{"status": 404}}',
            reason='Bad request'
        )

        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            resp = self.get_hook.run('v1/test', extra_options={'check_response': False})
            self.assertEqual(resp.text, '{"status":{"status": 404}}')

    @requests_mock.mock()
    def test_hook_contains_header_from_extra_field(self, mock_requests):
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            expected_conn = get_airflow_connection()
            conn = self.get_hook.get_conn()
            self.assertDictContainsSubset(json.loads(expected_conn.extra), conn.headers)
            self.assertEqual(conn.headers.get('bareer'), 'test')

    @requests_mock.mock()
    @mock.patch('requests.Request')
    def test_hook_with_method_in_lowercase(self, mock_requests, request_mock):
        from requests.exceptions import MissingSchema, InvalidURL
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection_with_port
        ):
            data = "test params"
            try:
                self.get_lowercase_hook.run('v1/test', data=data)
            except (MissingSchema, InvalidURL):
                pass
            request_mock.assert_called_once_with(
                mock.ANY,
                mock.ANY,
                headers=mock.ANY,
                params=data
            )

    @requests_mock.mock()
    def test_hook_uses_provided_header(self, mock_requests):
        conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
        self.assertEqual(conn.headers.get('bareer'), "newT0k3n")

    @requests_mock.mock()
    def test_hook_has_no_header_from_extra(self, mock_requests):
        conn = self.get_hook.get_conn()
        self.assertIsNone(conn.headers.get('bareer'))

    @requests_mock.mock()
    def test_hooks_header_from_extra_is_overridden(self, mock_requests):
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
            self.assertEqual(conn.headers.get('bareer'), 'newT0k3n')

    @requests_mock.mock()
    def test_post_request(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test',
            status_code=200,
            text='{"status":{"status": 200}}',
            reason='OK'
        )

        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            resp = self.post_hook.run('v1/test')
            self.assertEqual(resp.status_code, 200)

    @requests_mock.mock()
    def test_post_request_with_error_code(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test',
            status_code=418,
            text='{"status":{"status": 418}}',
            reason='I\'m a teapot'
        )

        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            with self.assertRaises(AirflowException):
                self.post_hook.run('v1/test')

    @requests_mock.mock()
    def test_post_request_do_not_raise_for_status_if_check_response_is_false(self, mock_requests):
        mock_requests.post(
            'http://test:8080/v1/test',
            status_code=418,
            text='{"status":{"status": 418}}',
            reason='I\'m a teapot'
        )

        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            resp = self.post_hook.run('v1/test', extra_options={'check_response': False})
            self.assertEqual(resp.status_code, 418)

    @mock.patch('airflow.hooks.http_hook.requests.Session')
    def test_retry_on_conn_error(self, mocked_session):

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(7),
            retry=tenacity.retry_if_exception_type(
                requests.exceptions.ConnectionError
            )
        )

        def send_and_raise(unused_request, **kwargs):
            raise requests.exceptions.ConnectionError

        mocked_session().send.side_effect = send_and_raise
        # The job failed for some reason
        with self.assertRaises(tenacity.RetryError):
            self.get_hook.run_with_advanced_retry(
                endpoint='v1/test',
                _retry_args=retry_args
            )
        self.assertEqual(
            self.get_hook._retry_obj.stop.max_attempt_number + 1,
            mocked_session.call_count
        )

    @requests_mock.mock()
    def test_run_with_advanced_retry(self, m):

        m.get(
            'http://test:8080/v1/test',
            status_code=200,
            reason='OK'
        )

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(3),
            retry=tenacity.retry_if_exception_type(Exception),
            reraise=True
        )
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            response = self.get_hook.run_with_advanced_retry(
                endpoint='v1/test',
                _retry_args=retry_args
            )
            self.assertIsInstance(response, requests.Response)

    def test_header_from_extra_and_run_method_are_merged(self):

        def run_and_return(unused_session, prepped_request, unused_extra_options, **kwargs):
            return prepped_request

        # The job failed for some reason
        with mock.patch(
            'airflow.hooks.http_hook.HttpHook.run_and_check',
            side_effect=run_and_return
        ):
            with mock.patch(
                'airflow.hooks.base_hook.BaseHook.get_connection',
                side_effect=get_airflow_connection
            ):
                pr = self.get_hook.run('v1/test', headers={'some_other_header': 'test'})
                actual = dict(pr.headers)
                self.assertEqual(actual.get('bareer'), 'test')
                self.assertEqual(actual.get('some_other_header'), 'test')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_http_connection(self, mock_get_connection):
        c = Connection(conn_id='http_default', conn_type='http',
                       host='localhost', schema='http')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'http://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_https_connection(self, mock_get_connection):
        c = Connection(conn_id='http_default', conn_type='http',
                       host='localhost', schema='https')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'https://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_host_encoded_http_connection(self, mock_get_connection):
        c = Connection(conn_id='http_default', conn_type='http',
                       host='http://localhost')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'http://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_host_encoded_https_connection(self, mock_get_connection):
        c = Connection(conn_id='http_default', conn_type='http',
                       host='https://localhost')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'https://localhost')

    def test_method_converted_to_uppercase_when_created_in_lowercase(self):
        self.assertEqual(self.get_lowercase_hook.method, 'GET')


send_email_test = mock.Mock()
