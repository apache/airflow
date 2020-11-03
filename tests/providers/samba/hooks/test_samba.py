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
from unittest.mock import call

from unittest import mock
import smbclient

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.samba.hooks.samba import SambaHook

connection = Connection(host='ip', schema='share', login='username', password='password')


class TestSambaHook(unittest.TestCase):
    def test_get_conn_should_fail_if_conn_id_does_not_exist(self):
        with self.assertRaises(AirflowException):
            SambaHook('conn')

    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_get_conn(self, get_conn_mock):
        get_conn_mock.return_value = connection
        hook = SambaHook('samba_default')

        self.assertEqual(smbclient.SambaClient, type(hook.get_conn()))
        get_conn_mock.assert_called_once_with('samba_default')

    @mock.patch('airflow.providers.samba.hooks.samba.SambaHook.get_conn')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_push_from_local_should_succeed_if_destination_has_same_name_but_not_a_file(
        self, base_conn_mock, samba_hook_mock
    ):
        base_conn_mock.return_value = connection
        samba_hook_mock.get_conn.return_value = mock.Mock()

        samba_hook_mock.return_value.exists.return_value = True
        samba_hook_mock.return_value.isfile.return_value = False
        samba_hook_mock.return_value.exists.return_value = True

        hook = SambaHook('samba_default')
        destination_filepath = "/path/to/dest/file"
        local_filepath = "/path/to/local/file"
        hook.push_from_local(destination_filepath=destination_filepath, local_filepath=local_filepath)

        base_conn_mock.assert_called_once_with('samba_default')
        samba_hook_mock.assert_called_once()
        samba_hook_mock.return_value.exists.assert_called_once_with(destination_filepath)
        samba_hook_mock.return_value.isfile.assert_called_once_with(destination_filepath)
        samba_hook_mock.return_value.remove.assert_not_called()
        samba_hook_mock.return_value.upload.assert_called_once_with(local_filepath, destination_filepath)

    @mock.patch('airflow.providers.samba.hooks.samba.SambaHook.get_conn')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_push_from_local_should_delete_file_if_exists_and_save_file(
        self, base_conn_mock, samba_hook_mock
    ):
        base_conn_mock.return_value = connection
        samba_hook_mock.get_conn.return_value = mock.Mock()

        samba_hook_mock.return_value.exists.return_value = False
        samba_hook_mock.return_value.exists.return_value = False

        hook = SambaHook('samba_default')
        destination_folder = "/path/to/dest"
        destination_filepath = destination_folder + "/file"
        local_filepath = "/path/to/local/file"
        hook.push_from_local(destination_filepath=destination_filepath, local_filepath=local_filepath)

        base_conn_mock.assert_called_once_with('samba_default')
        samba_hook_mock.assert_called_once()
        samba_hook_mock.return_value.exists.assert_has_calls(
            [call(destination_filepath), call(destination_folder)]
        )
        samba_hook_mock.return_value.isfile.assert_not_called()
        samba_hook_mock.return_value.remove.assert_not_called()
        samba_hook_mock.return_value.mkdir.assert_called_once_with(destination_folder)
        samba_hook_mock.return_value.upload.assert_called_once_with(local_filepath, destination_filepath)

    @mock.patch('airflow.providers.samba.hooks.samba.SambaHook.get_conn')
    @mock.patch('airflow.hooks.base_hook.BaseHook.get_connection')
    def test_push_from_local_should_create_directory_if_not_exist_and_save_file(
        self, base_conn_mock, samba_hook_mock
    ):
        base_conn_mock.return_value = connection
        samba_hook_mock.get_conn.return_value = mock.Mock()

        samba_hook_mock.return_value.exists.return_value = False
        samba_hook_mock.return_value.exists.return_value = False

        hook = SambaHook('samba_default')
        destination_folder = "/path/to/dest"
        destination_filepath = destination_folder + "/file"
        local_filepath = "/path/to/local/file"
        hook.push_from_local(destination_filepath=destination_filepath, local_filepath=local_filepath)

        base_conn_mock.assert_called_once_with('samba_default')
        samba_hook_mock.assert_called_once()
        samba_hook_mock.return_value.exists.assert_has_calls(
            [call(destination_filepath), call(destination_folder)]
        )
        samba_hook_mock.return_value.isfile.assert_not_called()
        samba_hook_mock.return_value.remove.assert_not_called()
        samba_hook_mock.return_value.mkdir.assert_called_once_with(destination_folder)
        samba_hook_mock.return_value.upload.assert_called_once_with(local_filepath, destination_filepath)
