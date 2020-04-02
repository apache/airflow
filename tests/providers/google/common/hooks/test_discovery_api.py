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
import unittest
from unittest.mock import call, patch

from airflow import models
from airflow.configuration import load_test_config
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook
from airflow.utils import db


class TestGoogleDiscoveryApiHook(unittest.TestCase):

    def setUp(self):
        load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id='google_test',
                host='google',
                schema='refresh_token',
                login='client_id',
                password='client_secret'
            )
        )

    @patch('airflow.providers.google.common.hooks.discovery_api.build')
    @patch('airflow.providers.google.common.hooks.discovery_api.GoogleDiscoveryApiHook._authorize')
    def test_get_conn(self, mock_authorize, mock_build):
        google_discovery_api_hook = GoogleDiscoveryApiHook(
            gcp_conn_id='google_test',
            api_service_name='youtube',
            api_version='v2'
        )

        google_discovery_api_hook.get_conn()

        mock_build.assert_called_once_with(
            serviceName=google_discovery_api_hook.api_service_name,
            version=google_discovery_api_hook.api_version,
            http=mock_authorize.return_value,
            cache_discovery=False
        )

    @patch('airflow.providers.google.common.hooks.discovery_api.getattr')
    @patch('airflow.providers.google.common.hooks.discovery_api.GoogleDiscoveryApiHook.get_conn')
    def test_query(self, mock_get_conn, mock_getattr):
        google_discovery_api_hook = GoogleDiscoveryApiHook(
            gcp_conn_id='google_test',
            api_service_name='analyticsreporting',
            api_version='v4'
        )
        endpoint = 'analyticsreporting.reports.batchGet'
        data = {
            'body': {
                'reportRequests': [{
                    'viewId': '180628393',
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:country'}]
                }]
            }
        }
        num_retries = 1

        google_discovery_api_hook.query(endpoint, data, num_retries=num_retries)

        google_api_endpoint_name_parts = endpoint.split('.')
        mock_getattr.assert_has_calls([
            call(mock_get_conn.return_value, google_api_endpoint_name_parts[1]),
            call()(),
            call(mock_getattr.return_value.return_value, google_api_endpoint_name_parts[2]),
            call()(**data),
            call()().execute(num_retries=num_retries)
        ])

    @patch('airflow.providers.google.common.hooks.discovery_api.getattr')
    @patch('airflow.providers.google.common.hooks.discovery_api.GoogleDiscoveryApiHook.get_conn')
    def test_query_with_pagination(self, mock_get_conn, mock_getattr):
        google_api_conn_client_sub_call = mock_getattr.return_value.return_value
        mock_getattr.return_value.side_effect = [
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            google_api_conn_client_sub_call,
            None
        ]
        google_discovery_api_hook = GoogleDiscoveryApiHook(
            gcp_conn_id='google_test',
            api_service_name='analyticsreporting',
            api_version='v4'
        )
        endpoint = 'analyticsreporting.reports.batchGet'
        data = {
            'body': {
                'reportRequests': [{
                    'viewId': '180628393',
                    'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:sessions'}],
                    'dimensions': [{'name': 'ga:country'}]
                }]
            }
        }
        num_retries = 1

        google_discovery_api_hook.query(endpoint, data, paginate=True, num_retries=num_retries)

        api_endpoint_name_parts = endpoint.split('.')
        google_api_conn_client = mock_get_conn.return_value
        mock_getattr.assert_has_calls([
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2]),
            call()(**data),
            call()().__bool__(),
            call()().execute(num_retries=num_retries),
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2] + '_next'),
            call()(google_api_conn_client_sub_call, google_api_conn_client_sub_call.execute.return_value),
            call()().__bool__(),
            call()().execute(num_retries=num_retries),
            call(google_api_conn_client, api_endpoint_name_parts[1]),
            call()(),
            call(google_api_conn_client_sub_call, api_endpoint_name_parts[2] + '_next'),
            call()(google_api_conn_client_sub_call, google_api_conn_client_sub_call.execute.return_value)
        ])


if __name__ == '__main__':
    unittest.main()
