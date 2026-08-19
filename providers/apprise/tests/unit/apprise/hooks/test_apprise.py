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

import json
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, call, patch

import apprise
import pytest
from apprise import NotifyFormat, NotifyType, PersistentStoreMode

from airflow.models import Connection
from airflow.providers.apprise.hooks.apprise import AppriseHook


class TestAppriseHook:
    """
    Test for AppriseHook
    """

    @pytest.mark.parametrize(
        "config",
        [
            {"path": "http://some_path_that_dont_exist/", "tag": "alert"},
            '{"path": "http://some_path_that_dont_exist/", "tag": "alert"}',
        ],
    )
    def test_get_config_from_conn(self, config):
        extra = {"config": config}
        conn = Connection(conn_type="apprise", extra=extra)
        hook = AppriseHook()
        assert hook.get_config_from_conn(conn) == (json.loads(config) if isinstance(config, str) else config)

    def test_set_config_from_conn_with_dict(self):
        """
        Test set_config_from_conn for dict config
        """
        extra = {"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}}
        apprise_obj = apprise.Apprise()
        apprise_obj.add = MagicMock()
        conn = Connection(conn_type="apprise", extra=extra)
        hook = AppriseHook()
        hook.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)

        apprise_obj.add.assert_called_once_with("http://some_path_that_dont_exist/", tag="alert")

    def test_set_config_from_conn_with_list(self):
        """
        Test set_config_from_conn for list of dict config
        """
        extra = {
            "config": [
                {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                {"path": "http://some_other_path_that_dont_exist/", "tag": "p1"},
            ]
        }

        apprise_obj = apprise.Apprise()
        apprise_obj.add = MagicMock()
        conn = Connection(conn_type="apprise", extra=extra)
        hook = AppriseHook()
        hook.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)

        apprise_obj.add.assert_has_calls(
            [
                call("http://some_path_that_dont_exist/", tag="p0"),
                call("http://some_other_path_that_dont_exist/", tag="p1"),
            ]
        )

    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
    )
    def test_notify(self, mock_conn):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={
                "config": [
                    {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                    {"path": "http://some_other_path_that_dont_exist/", "tag": "p1"},
                ]
            },
        )
        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj):
            hook = AppriseHook()
            hook.notify(body="test")

        apprise_obj.notify.assert_called_once_with(
            body="test",
            title="",
            notify_type=NotifyType.INFO,
            body_format=NotifyFormat.TEXT,
            tag="all",
            attach=None,
            interpret_escapes=None,
        )

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.get_async_connection",
    )
    async def test_async_notify(self, mock_conn):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={
                "config": [
                    {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                    {"path": "http://some_other_path_that_dont_exist/", "tag": "p1"},
                ]
            },
        )
        apprise_obj = apprise.Apprise()
        apprise_obj.async_notify = AsyncMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj):
            hook = AppriseHook()
            await hook.async_notify(body="test")

        mock_conn.assert_called()
        apprise_obj.async_notify.assert_called_once_with(
            body="test",
            title="",
            notify_type=NotifyType.INFO,
            body_format=NotifyFormat.TEXT,
            tag="all",
            attach=None,
            interpret_escapes=None,
        )

    def test_build_apprise_asset_returns_none_without_storage_path(self):
        """Persistent storage stays disabled, matching previous behavior, unless storage_path is set."""
        hook = AppriseHook()
        assert hook._build_apprise_asset() is None

    @pytest.mark.parametrize(
        "storage_mode",
        [None, "flush", PersistentStoreMode.MEMORY],
    )
    @mock.patch("airflow.providers.apprise.hooks.apprise.AppriseAsset", autospec=True)
    def test_build_apprise_asset_with_storage_path(self, mock_asset_cls, storage_mode):
        hook = AppriseHook(storage_path="/tmp/apprise-cache", storage_mode=storage_mode)

        asset = hook._build_apprise_asset()

        mock_asset_cls.assert_called_once_with(storage_path="/tmp/apprise-cache", storage_mode=storage_mode)
        assert asset is mock_asset_cls.return_value

    @mock.patch("airflow.providers.apprise.hooks.apprise.AppriseHook._build_apprise_asset")
    @mock.patch("airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection")
    def test_notify_uses_built_apprise_asset(self, mock_conn, mock_build_asset):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}},
        )
        sentinel_asset = object()
        mock_build_asset.return_value = sentinel_asset

        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise_cls:
            hook = AppriseHook(storage_path="/tmp/apprise-cache")
            hook.notify(body="test")

        mock_apprise_cls.assert_called_once_with(asset=sentinel_asset)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apprise.hooks.apprise.AppriseHook._build_apprise_asset")
    @mock.patch("airflow.providers.apprise.hooks.apprise.get_async_connection")
    async def test_async_notify_uses_built_apprise_asset(self, mock_conn, mock_build_asset):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}},
        )
        sentinel_asset = object()
        mock_build_asset.return_value = sentinel_asset

        apprise_obj = apprise.Apprise()
        apprise_obj.async_notify = AsyncMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise_cls:
            hook = AppriseHook(storage_path="/tmp/apprise-cache")
            await hook.async_notify(body="test")

        mock_apprise_cls.assert_called_once_with(asset=sentinel_asset)
