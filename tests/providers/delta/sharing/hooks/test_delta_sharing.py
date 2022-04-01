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
#

import itertools
import unittest
from json import JSONDecodeError
from unittest import mock

import pytest
import tenacity
from requests import exceptions as requests_exceptions

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.delta.sharing.hooks.delta_sharing import DeltaSharingHook
from airflow.utils import db
from airflow.utils.session import provide_session

TASK_ID = 'delta-sharing-operator'
DEFAULT_CONN_ID = 'delta_sharing_default'
DEFAULT_SHARE = "share1"
DEFAULT_SCHEMA = "schema1"
DEFAULT_TABLE = "table1"
DEFAULT_RETRY_NUMBER = 3
DEFAULT_RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(DEFAULT_RETRY_NUMBER),
)
DELTA_SHARING_BASE_URL = 'https://xx.cloud.databricks.com/delta-sharing/'
TOKEN = 'token'
DEFAULT_HEADERS = {'user-agent': f'airflow-{__version__}', 'Authorization': f'Bearer {TOKEN}'}
TABLE_VERSION_ENDPOINT = f"shares/{DEFAULT_SHARE}/schemas/{DEFAULT_SCHEMA}/tables/{DEFAULT_TABLE}"
TABLE_QUERY_ENDPOINT = f"shares/{DEFAULT_SHARE}/schemas/{DEFAULT_SCHEMA}/tables/{DEFAULT_TABLE}/query"


def create_valid_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    return response


def create_successful_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response


def create_post_side_effect(exception, status_code=500):
    if exception != requests_exceptions.HTTPError:
        return exception()
    else:
        response = mock.MagicMock()
        response.status_code = status_code
        response.raise_for_status.side_effect = exception(response=response)
        return response


def setup_mock_requests(mock_requests, exception, status_code=500, error_count=None, response_content=None):
    side_effect = create_post_side_effect(exception, status_code)

    if error_count is None:
        # POST requests will fail indefinitely
        mock_requests.post.side_effect = itertools.repeat(side_effect)
    else:
        # POST requests will fail 'error_count' times, and then they will succeed (once)
        mock_requests.post.side_effect = [side_effect] * error_count + [
            create_valid_response_mock(response_content)
        ]


class TestDeltaSharingHook(unittest.TestCase):
    """
    Tests for DeltaSharingHook.
    """

    @provide_session
    def setUp(self, session=None):
        account_id_conn = Connection(
            conn_id=DeltaSharingHook.default_conn_name,
            conn_type=DeltaSharingHook.conn_type,
            host=DELTA_SHARING_BASE_URL,
            password=TOKEN,
        )

        db.merge_conn(account_id_conn)
        self.hook = DeltaSharingHook(retry_delay=0)

    def test_init_bad_retry_limit(self):
        with pytest.raises(ValueError):
            DeltaSharingHook(retry_limit=0)

    def test_do_api_call_retries_with_retryable_error(self):
        hook = DeltaSharingHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests') as mock_requests:
                with mock.patch.object(hook.log, 'error') as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        hook._do_api_call(TABLE_QUERY_ENDPOINT, {}, http_method='POST')

                    assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    def test_do_api_call_retries_with_too_many_requests(self):
        hook = DeltaSharingHook(retry_args=DEFAULT_RETRY_ARGS)

        with mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests') as mock_requests:
            with mock.patch.object(hook.log, 'error') as mock_errors:
                setup_mock_requests(mock_requests, requests_exceptions.HTTPError, status_code=429)

                with pytest.raises(AirflowException):
                    hook._do_api_call(TABLE_QUERY_ENDPOINT, {}, http_method='POST')

                assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_does_not_retry_with_non_retryable_error(self, mock_requests):
        hook = DeltaSharingHook(retry_args=DEFAULT_RETRY_ARGS)

        setup_mock_requests(mock_requests, requests_exceptions.HTTPError, status_code=400)

        with mock.patch.object(hook.log, 'error') as mock_errors:
            with pytest.raises(AirflowException):
                hook._do_api_call(TABLE_QUERY_ENDPOINT, {}, http_method='POST')

            mock_errors.assert_not_called()

    def test_do_api_call_succeeds_after_retrying(self):
        hook = DeltaSharingHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests') as mock_requests:
                with mock.patch.object(hook.log, 'error') as mock_errors:
                    setup_mock_requests(mock_requests, exception, error_count=2)

                    hook._do_api_call(TABLE_QUERY_ENDPOINT, {}, http_method='POST')

                    assert mock_errors.call_count == 2

    def test_do_api_call_custom_retry(self):
        hook = DeltaSharingHook(retry_args=DEFAULT_RETRY_ARGS)

        for exception in [
            requests_exceptions.ConnectionError,
            requests_exceptions.SSLError,
            requests_exceptions.Timeout,
            requests_exceptions.ConnectTimeout,
            requests_exceptions.HTTPError,
        ]:
            with mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests') as mock_requests:
                with mock.patch.object(hook.log, 'error') as mock_errors:
                    setup_mock_requests(mock_requests, exception)

                    with pytest.raises(AirflowException):
                        hook._do_api_call(TABLE_QUERY_ENDPOINT, {}, http_method='POST')

                    assert mock_errors.call_count == DEFAULT_RETRY_NUMBER

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_get_table_version(self, mock_requests):
        mock_requests.head.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
            ('delta-table-version', '123'),
        ]
        version = self.hook.get_table_version(DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE)

        assert version == 123
        mock_requests.head.assert_called_once_with(
            f"{DELTA_SHARING_BASE_URL}{TABLE_VERSION_ENDPOINT}",
            json=None,
            params=None,
            headers=DEFAULT_HEADERS,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_get_table_version_no_version(self, mock_requests):
        mock_requests.head.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
        ]
        with pytest.raises(AirflowException):
            self.hook.get_table_version(DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE)

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_query_table(self, mock_requests):
        mock_requests.post.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
            ('delta-table-version', '123'),
        ]
        mock_requests.post.return_value.text = (
            """{"protocol":{"minReaderVersion": 1}}\n"""
            """{"metaData":{"id":"776c5e42-9e86-1234-b6f1-999aa383c03f","format":{"provider":"parquet"},"""
            """"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"vendor_id\\","""
            """\\"type\\":\\"string\\",\\"nullable\\":true,\\"metadata\\":{}}]}","partitionColumns":[]}}\n"""
            """{"file":{"url":"https://xxxx.s3.us-west-2.amazonaws.com/samples/table/part-123.snappy"""
            """.parquet?X-Amz-Security-Token=123", "id": "1234", "partitionValues": {}, "size": 123}}\n"""
        )

        limit = 100
        predicates = ["date > '2022-03-31"]
        resp = self.hook.query_table(
            DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE, limit=limit, predicates=predicates
        )

        assert resp.version == 123
        assert resp.metadata == {
            "id": "776c5e42-9e86-1234-b6f1-999aa383c03f",
            "format": {"provider": "parquet"},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"vendor_id\","
            "\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
        }
        assert resp.files == [
            {
                "url": "https://xxxx.s3.us-west-2.amazonaws.com/samples/table/"
                "part-123.snappy.parquet?X-Amz-Security-Token=123",
                "id": "1234",
                "partitionValues": {},
                "size": 123,
            }
        ]
        mock_requests.post.assert_called_once_with(
            f"{DELTA_SHARING_BASE_URL}{TABLE_QUERY_ENDPOINT}",
            json={"limitHint": limit, "predicateHints": predicates},
            params=None,
            headers=DEFAULT_HEADERS,
            timeout=self.hook.timeout_seconds,
        )

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_query_table_no_version(self, mock_requests):
        mock_requests.post.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
        ]
        with pytest.raises(AirflowException):
            self.hook.query_table(DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE)

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_query_table_too_few_data(self, mock_requests):
        mock_requests.post.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
            ('delta-table-version', '123'),
        ]
        mock_requests.post.return_value.text = """{"protocol": {"minReaderVersion": 1}}
"""
        with pytest.raises(AirflowException):
            self.hook.query_table(DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE)

    @mock.patch('airflow.providers.delta.sharing.hooks.delta_sharing.requests')
    def test_do_api_call_query_table_garbage_data(self, mock_requests):
        mock_requests.post.return_value.headers.lower_items.return_value = [
            ('content-type', 'application/json'),
            ('delta-table-version', '123'),
        ]
        mock_requests.post.return_value.text = """{"protocol": {"minReaderVersion": 1}}\nadaada\nsdadads"""
        with pytest.raises(JSONDecodeError):
            self.hook.query_table(DEFAULT_SHARE, DEFAULT_SCHEMA, DEFAULT_TABLE)
