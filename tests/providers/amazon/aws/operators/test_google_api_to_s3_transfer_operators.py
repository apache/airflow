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
from unittest.mock import Mock, patch

from airflow import models
from airflow.configuration import load_test_config
from airflow.models.xcom import MAX_XCOM_SIZE
from airflow.providers.amazon.aws.operators.google_api_to_s3_transfer import GoogleApiToS3Transfer
from airflow.utils import db


class TestGoogleApiToS3Transfer(unittest.TestCase):

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
        db.merge_conn(
            models.Connection(
                conn_id='s3_test',
                conn_type='s3',
                schema='test',
                extra='{"aws_access_key_id": "aws_access_key_id", "aws_secret_access_key":'
                      ' "aws_secret_access_key"}'
            )
        )

        self.kwargs = {
            'gcp_conn_id': 'google_test',
            'google_api_service_name': 'test_service',
            'google_api_service_version': 'v3',
            'google_api_endpoint_path': 'analyticsreporting.reports.batchGet',
            'google_api_endpoint_params': {},
            'google_api_pagination': False,
            'google_api_num_retries': 0,
            'aws_conn_id': 's3_test',
            's3_destination_key': 'test/google_api_to_s3_test.csv',
            's3_overwrite': True,
            'task_id': 'task_id',
            'dag': None
        }

    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.GoogleDiscoveryApiHook.query')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.S3Hook.load_string')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.json.dumps')
    def test_execute(self, mock_json_dumps, mock_s3_hook_load_string, mock_google_api_hook_query):
        context = {'task_instance': Mock()}

        GoogleApiToS3Transfer(**self.kwargs).execute(context)

        mock_google_api_hook_query.assert_called_once_with(
            endpoint=self.kwargs['google_api_endpoint_path'],
            data=self.kwargs['google_api_endpoint_params'],
            paginate=self.kwargs['google_api_pagination'],
            num_retries=self.kwargs['google_api_num_retries']
        )
        mock_json_dumps.assert_called_once_with(mock_google_api_hook_query.return_value)
        mock_s3_hook_load_string.assert_called_once_with(
            string_data=mock_json_dumps.return_value,
            key=self.kwargs['s3_destination_key'],
            replace=self.kwargs['s3_overwrite']
        )
        context['task_instance'].xcom_pull.assert_not_called()
        context['task_instance'].xcom_push.assert_not_called()

    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.GoogleDiscoveryApiHook.query')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.S3Hook.load_string')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.json.dumps')
    def test_execute_with_xcom(self, mock_json_dumps, mock_s3_hook_load_string, mock_google_api_hook_query):
        context = {'task_instance': Mock()}
        xcom_kwargs = {
            'google_api_response_via_xcom': 'response',
            'google_api_endpoint_params_via_xcom': 'params',
            'google_api_endpoint_params_via_xcom_task_ids': 'params',
        }
        context['task_instance'].xcom_pull.return_value = {}

        GoogleApiToS3Transfer(**self.kwargs, **xcom_kwargs).execute(context)

        mock_google_api_hook_query.assert_called_once_with(
            endpoint=self.kwargs['google_api_endpoint_path'],
            data=self.kwargs['google_api_endpoint_params'],
            paginate=self.kwargs['google_api_pagination'],
            num_retries=self.kwargs['google_api_num_retries']
        )
        mock_json_dumps.assert_called_once_with(mock_google_api_hook_query.return_value)
        mock_s3_hook_load_string.assert_called_once_with(
            string_data=mock_json_dumps.return_value,
            key=self.kwargs['s3_destination_key'],
            replace=self.kwargs['s3_overwrite']
        )
        context['task_instance'].xcom_pull.assert_called_once_with(
            task_ids=xcom_kwargs['google_api_endpoint_params_via_xcom_task_ids'],
            key=xcom_kwargs['google_api_endpoint_params_via_xcom']
        )
        context['task_instance'].xcom_push.assert_called_once_with(
            key=xcom_kwargs['google_api_response_via_xcom'],
            value=mock_google_api_hook_query.return_value
        )

    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.GoogleDiscoveryApiHook.query')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.S3Hook.load_string')
    @patch('airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.json.dumps')
    @patch(
        'airflow.providers.amazon.aws.operators.google_api_to_s3_transfer.sys.getsizeof',
        return_value=MAX_XCOM_SIZE
    )
    def test_execute_with_xcom_exceeded_max_xcom_size(
        self,
        mock_sys_getsizeof,
        mock_json_dumps,
        mock_s3_hook_load_string,
        mock_google_api_hook_query
    ):
        context = {'task_instance': Mock()}
        xcom_kwargs = {
            'google_api_response_via_xcom': 'response',
            'google_api_endpoint_params_via_xcom': 'params',
            'google_api_endpoint_params_via_xcom_task_ids': 'params',
        }
        context['task_instance'].xcom_pull.return_value = {}

        self.assertRaises(RuntimeError, GoogleApiToS3Transfer(**self.kwargs, **xcom_kwargs).execute, context)

        mock_google_api_hook_query.assert_called_once_with(
            endpoint=self.kwargs['google_api_endpoint_path'],
            data=self.kwargs['google_api_endpoint_params'],
            paginate=self.kwargs['google_api_pagination'],
            num_retries=self.kwargs['google_api_num_retries']
        )
        mock_json_dumps.assert_called_once_with(mock_google_api_hook_query.return_value)
        mock_s3_hook_load_string.assert_called_once_with(
            string_data=mock_json_dumps.return_value,
            key=self.kwargs['s3_destination_key'],
            replace=self.kwargs['s3_overwrite']
        )
        context['task_instance'].xcom_pull.assert_called_once_with(
            task_ids=xcom_kwargs['google_api_endpoint_params_via_xcom_task_ids'],
            key=xcom_kwargs['google_api_endpoint_params_via_xcom']
        )
        context['task_instance'].xcom_push.assert_not_called()
        mock_sys_getsizeof.assert_called_once_with(mock_google_api_hook_query.return_value)


if __name__ == '__main__':
    unittest.main()
