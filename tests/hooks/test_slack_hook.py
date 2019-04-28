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

import unittest
from airflow.exceptions import AirflowException
from airflow.hooks.slack_hook import SlackHook
from tests.compat import mock


class SlackHookTestCase(unittest.TestCase):
    def test_init_with_token_only(self):
        test_token = 'test_token'
        slack_hook = SlackHook(token=test_token, slack_conn_id=None)

        self.assertEqual(slack_hook.token, test_token)

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test_init_with_valid_slack_conn_id_only(self, get_connection_mock):
        test_password = 'test_password'
        get_connection_mock.return_value = mock.Mock(password=test_password)

        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=None, slack_conn_id=test_slack_conn_id)

        get_connection_mock.assert_called_with(test_slack_conn_id)
        self.assertEqual(slack_hook.token, test_password)

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test_init_with_no_password_slack_conn_id_only(self, get_connection_mock):
        conn = mock.Mock()
        del conn.password
        get_connection_mock.return_value = conn

        test_slack_conn_id = 'test_slack_conn_id'
        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id=test_slack_conn_id)

    @mock.patch('airflow.hooks.slack_hook.SlackHook.get_connection')
    def test_init_with_empty_password_slack_conn_id_only(self, get_connection_mock):
        get_connection_mock.return_value = mock.Mock(password=None)

        test_slack_conn_id = 'test_slack_conn_id'
        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id=test_slack_conn_id)

    def test_init_with_token_and_slack_conn_id(self):
        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)

        self.assertEqual(slack_hook.token, test_token)

    def test_init_with_out_token_nor_slack_conn_id(self):
        self.assertRaises(AirflowException, SlackHook, token=None, slack_conn_id=None)

    @mock.patch('airflow.hooks.slack_hook.SlackClient')
    def test_call_with_success(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        slack_client_mock.api_call.return_value = {'ok': True}

        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)
        test_method = 'test_method'
        test_api_params = {'key1': 'value1', 'key2': 'value2'}

        slack_hook.call(test_method, test_api_params)

        slack_client_class_mock.assert_called_with(test_token)
        slack_client_mock.api_call.assert_called_with(test_method, **test_api_params)

    @mock.patch('airflow.hooks.slack_hook.SlackClient')
    def test_call_with_failure(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        slack_client_mock.api_call.return_value = {'ok': False, 'error': 'test_error'}

        test_token = 'test_token'
        test_slack_conn_id = 'test_slack_conn_id'
        slack_hook = SlackHook(token=test_token, slack_conn_id=test_slack_conn_id)
        test_method = 'test_method'
        test_api_params = {'key1': 'value1', 'key2': 'value2'}

        self.assertRaises(AirflowException, slack_hook.call, test_method, test_api_params)
