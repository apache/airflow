# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

import json

import requests
import requests_mock

import tenacity

from airflow import configuration, models
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


def get_airflow_connection(conn_id=None):
    return models.Connection(
        conn_id='http_default',
        conn_type='http',
        host='test:8080/',
        extra='{"bareer": "test"}'
    )


class TestHttpHook(unittest.TestCase):
    """Test get, post and raise_for_status"""
    def setUp(self):
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)
        self.get_hook = HttpHook(method='GET')
        self.post_hook = HttpHook(method='POST')
        configuration.load_test_config()

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
            self.assertEquals(resp.text, '{"status":{"status": 200}}')

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
            self.assertEquals(resp.text, '{"status":{"status": 404}}')

    @requests_mock.mock()
    def test_hook_contains_header_from_extra_field(self, m):
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            expected_conn = get_airflow_connection()
            conn = self.get_hook.get_conn()
            self.assertDictContainsSubset(json.loads(expected_conn.extra), conn.headers)
            self.assertEquals(conn.headers.get('bareer'), 'test')

    @requests_mock.mock()
    def test_hook_uses_provided_header(self, m):
            conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
            self.assertEquals(conn.headers.get('bareer'), "newT0k3n")

    @requests_mock.mock()
    def test_hook_has_no_header_from_extra(self, m):
            conn = self.get_hook.get_conn()
            self.assertIsNone(conn.headers.get('bareer'))

    @requests_mock.mock()
    def test_hooks_header_from_extra_is_overridden(self, m):
        with mock.patch(
            'airflow.hooks.base_hook.BaseHook.get_connection',
            side_effect=get_airflow_connection
        ):
            conn = self.get_hook.get_conn(headers={"bareer": "newT0k3n"})
            self.assertEquals(conn.headers.get('bareer'), 'newT0k3n')

    @requests_mock.mock()
    def test_post_request(self, m):

        m.post(
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
            self.assertEquals(resp.status_code, 200)

    @requests_mock.mock()
    def test_post_request_with_error_code(self, m):

        m.post(
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
    def test_post_request_do_not_raise_for_status_if_check_response_is_false(self, m):

        m.post(
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
            self.assertEquals(resp.status_code, 418)

    @mock.patch('airflow.hooks.http_hook.requests.Session')
    def test_retry_on_conn_error(self, mocked_session):

        retry_args = dict(
            wait=tenacity.wait_none(),
            stop=tenacity.stop_after_attempt(7),
            retry=requests.exceptions.ConnectionError
        )

        def send_and_raise(request, **kwargs):
            raise requests.exceptions.ConnectionError

        mocked_session().send.side_effect = send_and_raise
        # The job failed for some reason
        with self.assertRaises(tenacity.RetryError):
            self.get_hook.run_with_advanced_retry(
                endpoint='v1/test',
                _retry_args=retry_args
            )
        self.assertEquals(
            self.get_hook._retry_obj.stop.max_attempt_number + 1,
            mocked_session.call_count
        )

    def test_header_from_extra_and_run_method_are_merged(self):

        def run_and_return(session, prepped_request, extra_options, **kwargs):
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
                self.assertEquals(actual.get('bareer'), 'test')
                self.assertEquals(actual.get('some_other_header'), 'test')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_http_connection(self, mock_get_connection):
        c = models.Connection(conn_id='http_default', conn_type='http',
                              host='localhost', schema='http')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'http://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_https_connection(self, mock_get_connection):
        c = models.Connection(conn_id='http_default', conn_type='http',
                              host='localhost', schema='https')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'https://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_host_encoded_http_connection(self, mock_get_connection):
        c = models.Connection(conn_id='http_default', conn_type='http',
                              host='http://localhost')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'http://localhost')

    @mock.patch('airflow.hooks.http_hook.HttpHook.get_connection')
    def test_host_encoded_https_connection(self, mock_get_connection):
        c = models.Connection(conn_id='http_default', conn_type='http',
                              host='https://localhost')
        mock_get_connection.return_value = c
        hook = HttpHook()
        hook.get_conn({})
        self.assertEqual(hook.base_url, 'https://localhost')


send_email_test = mock.Mock()


if __name__ == '__main__':
    unittest.main()
