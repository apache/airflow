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
#

import unittest

import mock

from airflow.hooks.zendesk_hook import ZendeskHook
from zdesk import RateLimitError


class TestZendeskHook(unittest.TestCase):

    @mock.patch("airflow.hooks.zendesk_hook.time")
    def test_sleeps_for_correct_interval(self, mocked_time):
        sleep_time = 10
        # To break out of the otherwise infinite tries
        mocked_time.sleep = mock.Mock(side_effect=ValueError, return_value=3)
        conn_mock = mock.Mock()
        mock_response = mock.Mock()
        mock_response.headers.get.return_value = sleep_time
        conn_mock.call = mock.Mock(
            side_effect=RateLimitError(msg="some message",
                                       code="some code",
                                       response=mock_response))

        zendesk_hook = ZendeskHook("conn_id")
        zendesk_hook.get_conn = mock.Mock(return_value=conn_mock)

        with self.assertRaises(ValueError):
            zendesk_hook.call("some_path", get_all_pages=False)
            mocked_time.sleep.assert_called_with(sleep_time)

    @mock.patch("airflow.hooks.zendesk_hook.Zendesk")
    def test_returns_single_page_if_get_all_pages_false(self, _):
        zendesk_hook = ZendeskHook("conn_id")
        mock_connection = mock.Mock()
        mock_connection.host = "some_host"
        zendesk_hook.get_connection = mock.Mock(return_value=mock_connection)
        zendesk_hook.get_conn()

        mock_conn = mock.Mock()
        mock_call = mock.Mock(
            return_value={'next_page': 'https://some_host/something',
                          'path': []})
        mock_conn.call = mock_call
        zendesk_hook.get_conn = mock.Mock(return_value=mock_conn)
        zendesk_hook.call("path", get_all_pages=False)
        mock_call.assert_called_once_with("path", None)

    @mock.patch("airflow.hooks.zendesk_hook.Zendesk")
    def test_returns_multiple_pages_if_get_all_pages_true(self, _):
        zendesk_hook = ZendeskHook("conn_id")
        mock_connection = mock.Mock()
        mock_connection.host = "some_host"
        zendesk_hook.get_connection = mock.Mock(return_value=mock_connection)
        zendesk_hook.get_conn()

        mock_conn = mock.Mock()
        mock_call = mock.Mock(
            return_value={'next_page': 'https://some_host/something',
                          'path': []})
        mock_conn.call = mock_call
        zendesk_hook.get_conn = mock.Mock(return_value=mock_conn)
        zendesk_hook.call("path", get_all_pages=True)
        assert mock_call.call_count == 2

    @mock.patch("airflow.hooks.zendesk_hook.Zendesk")
    def test_zdesk_is_inited_correctly(self, mock_zendesk):
        conn_mock = mock.Mock()
        conn_mock.host = "conn_host"
        conn_mock.login = "conn_login"
        conn_mock.password = "conn_pass"

        zendesk_hook = ZendeskHook("conn_id")
        zendesk_hook.get_connection = mock.Mock(return_value=conn_mock)
        zendesk_hook.get_conn()
        mock_zendesk.assert_called_with('https://conn_host', 'conn_login',
                                        'conn_pass', True)

    @mock.patch("airflow.hooks.zendesk_hook.Zendesk")
    def test_zdesk_sideloading_works_correctly(self, mock_zendesk):
        zendesk_hook = ZendeskHook("conn_id")
        mock_connection = mock.Mock()
        mock_connection.host = "some_host"
        zendesk_hook.get_connection = mock.Mock(return_value=mock_connection)
        zendesk_hook.get_conn()

        mock_conn = mock.Mock()
        mock_call = mock.Mock(
            return_value={'next_page': 'https://some_host/something',
                          'tickets': [],
                          'users': [],
                          'groups': []})
        mock_conn.call = mock_call
        zendesk_hook.get_conn = mock.Mock(return_value=mock_conn)
        results = zendesk_hook.call(".../tickets.json",
                                    query={"include": "users,groups"},
                                    get_all_pages=False,
                                    side_loading=True)
        assert results == {'groups': [], 'users': [], 'tickets': []}
