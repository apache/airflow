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
from unittest import mock

import requests_mock

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.providers.openfaas.hooks.openfaas import OpenFaasHook

FUNCTION_NAME = "function_name"


class TestOpenFaasHook(unittest.TestCase):
    GET_FUNCTION = "/system/function/"
    INVOKE_ASYNC_FUNCTION = "/async-function/"
    DEPLOY_FUNCTION = "/system/functions"
    UPDATE_FUNCTION = "/system/functions"

    def setUp(self):
        self.hook = OpenFaasHook(function_name=FUNCTION_NAME)
        self.mock_response = {'ans': 'a'}

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_is_function_exist_false(self, mock_get_connection, m):
        m.get(
            "http://open-faas.io" + self.GET_FUNCTION + FUNCTION_NAME,
            json=self.mock_response,
            status_code=404,
        )
        mock_connection = Connection(host="http://open-faas.io")

        mock_get_connection.return_value = mock_connection
        does_function_exist = self.hook.does_function_exist()
        self.assertFalse(does_function_exist)

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_is_function_exist_true(self, mock_get_connection, m):
        m.get(
            "http://open-faas.io" + self.GET_FUNCTION + FUNCTION_NAME,
            json=self.mock_response,
            status_code=202,
        )
        mock_connection = Connection(host="http://open-faas.io")

        mock_get_connection.return_value = mock_connection
        does_function_exist = self.hook.does_function_exist()
        self.assertTrue(does_function_exist)

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_update_function_true(self, mock_get_connection, m):
        m.put("http://open-faas.io" + self.UPDATE_FUNCTION, json=self.mock_response, status_code=202)
        mock_connection = Connection(host="http://open-faas.io")

        mock_get_connection.return_value = mock_connection
        self.hook.update_function({})  # returns None

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_update_function_false(self, mock_get_connection, m):
        m.put("http://open-faas.io" + self.UPDATE_FUNCTION, json=self.mock_response, status_code=400)
        mock_connection = Connection(host="http://open-faas.io")
        mock_get_connection.return_value = mock_connection

        with self.assertRaises(AirflowException) as context:
            self.hook.update_function({})
        self.assertIn('failed to update ' + FUNCTION_NAME, str(context.exception))

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_invoke_async_function_false(self, mock_get_connection, m):
        m.post(
            "http://open-faas.io" + self.INVOKE_ASYNC_FUNCTION + FUNCTION_NAME,
            json=self.mock_response,
            status_code=400,
        )
        mock_connection = Connection(host="http://open-faas.io")
        mock_get_connection.return_value = mock_connection

        with self.assertRaises(AirflowException) as context:
            self.hook.invoke_async_function({})
        self.assertIn('failed to invoke function', str(context.exception))

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_invoke_async_function_true(self, mock_get_connection, m):
        m.post(
            "http://open-faas.io" + self.INVOKE_ASYNC_FUNCTION + FUNCTION_NAME,
            json=self.mock_response,
            status_code=202,
        )
        mock_connection = Connection(host="http://open-faas.io")
        mock_get_connection.return_value = mock_connection
        self.assertEqual(self.hook.invoke_async_function({}), None)

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_deploy_function_function_already_exist(self, mock_get_connection, m):
        m.put("http://open-faas.io/" + self.UPDATE_FUNCTION, json=self.mock_response, status_code=202)
        mock_connection = Connection(host="http://open-faas.io/")
        mock_get_connection.return_value = mock_connection
        self.assertEqual(self.hook.deploy_function(True, {}), None)

    @mock.patch.object(BaseHook, 'get_connection')
    @requests_mock.mock()
    def test_deploy_function_function_not_exist(self, mock_get_connection, m):
        m.post("http://open-faas.io" + self.DEPLOY_FUNCTION, json={}, status_code=202)
        mock_connection = Connection(host="http://open-faas.io")
        mock_get_connection.return_value = mock_connection
        self.assertEqual(self.hook.deploy_function(False, {}), None)
