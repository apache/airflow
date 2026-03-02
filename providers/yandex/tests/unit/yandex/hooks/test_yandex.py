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
from __future__ import annotations

from unittest import mock

import pytest

pytest.importorskip("yandexcloud")

from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook

from tests_common.test_utils.config import conf_vars

BASEHOOK_PATCH_PATH = "airflow.providers.common.compat.sdk.BaseHook"


class TestYandexHook:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    @mock.patch("airflow.providers.yandex.utils.credentials.get_credentials")
    def test_client_created_without_exceptions(self, mock_get_credentials, mock_get_connection):
        """tests `init` method to validate client creation when all parameters are passed"""

        default_folder_id = "test_id"
        default_public_ssh_key = "test_key"

        extra_dejson = '{"extras": "extra"}'
        mock_get_connection["extra_dejson"] = "sds"
        mock_get_connection.extra_dejson = '{"extras": "extra"}'
        mock_get_connection.return_value = mock.Mock(
            yandex_conn_id="yandexcloud_default", extra_dejson=extra_dejson
        )
        mock_get_credentials.return_value = {"token": 122323}

        hook = YandexCloudBaseHook(
            yandex_conn_id=None,
            default_folder_id=default_folder_id,
            default_public_ssh_key=default_public_ssh_key,
        )
        assert hook.client is not None

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    @mock.patch("airflow.providers.yandex.utils.credentials.get_credentials")
    def test_sdk_user_agent(self, mock_get_credentials, mock_get_connection):
        mock_get_connection.return_value = mock.Mock(yandex_conn_id="yandexcloud_default", extra_dejson="{}")
        mock_get_credentials.return_value = {"token": 122323}
        sdk_prefix = "MyAirflow"

        with conf_vars({("yandex", "sdk_user_agent_prefix"): sdk_prefix}):
            hook = YandexCloudBaseHook()
            assert hook.sdk._channels._client_user_agent.startswith(sdk_prefix)

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    @mock.patch("airflow.providers.yandex.utils.credentials.get_credentials")
    def test_get_endpoint_specified(self, mock_get_credentials, mock_get_connection):
        default_folder_id = "test_id"
        default_public_ssh_key = "test_key"

        extra_dejson = {"endpoint": "my_endpoint", "something_else": "some_value"}
        mock_get_connection.return_value = mock.Mock(
            yandex_conn_id="yandexcloud_default", extra_dejson=extra_dejson
        )
        mock_get_credentials.return_value = {"token": 122323}

        hook = YandexCloudBaseHook(
            yandex_conn_id=None,
            default_folder_id=default_folder_id,
            default_public_ssh_key=default_public_ssh_key,
        )

        assert hook._get_endpoint() == {"endpoint": "my_endpoint"}

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    @mock.patch("airflow.providers.yandex.utils.credentials.get_credentials")
    def test_get_endpoint_unspecified(self, mock_get_credentials, mock_get_connection):
        default_folder_id = "test_id"
        default_public_ssh_key = "test_key"

        extra_dejson = {"something_else": "some_value"}
        mock_get_connection.return_value = mock.Mock(
            yandex_conn_id="yandexcloud_default", extra_dejson=extra_dejson
        )
        mock_get_credentials.return_value = {"token": 122323}

        hook = YandexCloudBaseHook(
            yandex_conn_id=None,
            default_folder_id=default_folder_id,
            default_public_ssh_key=default_public_ssh_key,
        )

        assert hook._get_endpoint() == {}

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test__get_field(self, mock_get_connection):
        field_name = "one"
        field_value = "value_one"
        default_folder_id = "test_id"
        default_public_ssh_key = "test_key"
        extra_dejson = {field_name: field_value}

        mock_get_connection["extra_dejson"] = "sds"
        mock_get_connection.extra_dejson = '{"extras": "extra"}'
        mock_get_connection.return_value = mock.Mock(
            yandex_conn_id="yandexcloud_default", extra_dejson=extra_dejson
        )

        hook = YandexCloudBaseHook(
            yandex_conn_id=None,
            default_folder_id=default_folder_id,
            default_public_ssh_key=default_public_ssh_key,
        )
        res = hook._get_field(
            field_name=field_name,
        )

        assert res == field_value

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test__get_field_extras_not_found(self, get_connection_mock):
        field_name = "some_field"
        default = "some_default"
        extra_dejson = '{"extras": "extra"}'

        get_connection_mock["extra_dejson"] = "sds"
        get_connection_mock.extra_dejson = '{"extras": "extra"}'
        get_connection_mock.return_value = mock.Mock(
            yandex_conn_id="yandexcloud_default", extra_dejson=extra_dejson
        )

        hook = YandexCloudBaseHook()
        delattr(hook, "extras")
        res = hook._get_field(
            field_name=field_name,
            default=default,
        )

        assert res == default
