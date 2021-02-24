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
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook


class TestYandexHook(unittest.TestCase):
    @mock.patch('airflow.hooks.base.BaseHook.get_connection')
    @mock.patch('airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials')
    def test_client_created_without_exceptions(self, get_credentials_mock, get_connection_mock):
        """tests `init` method to validate client creation when all parameters are passed """

        # Inputs to constructor
        default_folder_id = 'test_id'
        default_public_ssh_key = 'test_key'

        extra_dejson = '{"extras": "extra"}'
        get_connection_mock['extra_dejson'] = "sdsd"
        get_connection_mock.extra_dejson = '{"extras": "extra"}'
        get_connection_mock.return_value = mock.Mock(
            connection_id='yandexcloud_default', extra_dejson=extra_dejson
        )
        get_credentials_mock.return_value = {"token": 122323}

        hook = YandexCloudBaseHook(None, default_folder_id, default_public_ssh_key)
        assert hook.client is not None

    @mock.patch('airflow.hooks.base.BaseHook.get_connection')
    def test_get_credentials_raise_exception(self, get_connection_mock):

        """tests 'get_credentials' method raising exception if none of the required fields are passed."""

        # Inputs to constructor
        default_folder_id = 'test_id'
        default_public_ssh_key = 'test_key'

        extra_dejson = '{"extras": "extra"}'
        get_connection_mock['extra_dejson'] = "sdsd"
        get_connection_mock.extra_dejson = '{"extras": "extra"}'
        get_connection_mock.return_value = mock.Mock(
            connection_id='yandexcloud_default', extra_dejson=extra_dejson
        )

        with pytest.raises(AirflowException):
            YandexCloudBaseHook(None, default_folder_id, default_public_ssh_key)

    @mock.patch('airflow.hooks.base.BaseHook.get_connection')
    @mock.patch('airflow.providers.yandex.hooks.yandex.YandexCloudBaseHook._get_credentials')
    def test_get_field(self, get_credentials_mock, get_connection_mock):
        # Inputs to constructor
        default_folder_id = 'test_id'
        default_public_ssh_key = 'test_key'

        extra_dejson = {"extra__yandexcloud__one": "value_one"}
        get_connection_mock['extra_dejson'] = "sdsd"
        get_connection_mock.extra_dejson = '{"extras": "extra"}'
        get_connection_mock.return_value = mock.Mock(
            connection_id='yandexcloud_default', extra_dejson=extra_dejson
        )
        get_credentials_mock.return_value = {"token": 122323}

        hook = YandexCloudBaseHook(None, default_folder_id, default_public_ssh_key)

        assert hook._get_field('one') == 'value_one'
