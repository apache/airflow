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
from apprise import NotifyFormat, NotifyType

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

    @pytest.mark.parametrize(
        ("storage_mode", "is_valid"),
        [("", True), ("auto", True), ("flush", True), ("memory", True), ("invalid", False)],
    )
    def test_get_connection_form_widgets_storage_fields(self, storage_mode, is_valid):
        pytest.importorskip("flask_appbuilder")
        pytest.importorskip("flask_babel")
        Form = pytest.importorskip("wtforms").Form
        MultiDict = pytest.importorskip("werkzeug.datastructures").MultiDict

        widgets = AppriseHook.get_connection_form_widgets()
        assert "storage_path" in widgets
        assert "storage_mode" in widgets

        form_cls = type("_AppriseConnForm", (Form,), dict(widgets))
        form = form_cls(MultiDict([("storage_path", "/tmp/apprise"), ("storage_mode", storage_mode)]))

        assert form.validate() is is_valid, form.errors

    def test_get_asset_returns_none_when_storage_path_unset(self):
        conn = Connection(conn_type="apprise", extra={"config": {"path": "http://some_path/"}})
        hook = AppriseHook()

        assert hook._get_asset(conn) is None
        assert hook._get_asset(None) is None

    def test_get_asset_uses_connection_extra(self, tmp_path):
        conn = Connection(
            conn_type="apprise",
            extra={"storage_path": str(tmp_path), "storage_mode": "flush"},
        )
        hook = AppriseHook()

        asset = hook._get_asset(conn)

        assert asset is not None
        assert asset.storage_path == str(tmp_path)
        assert asset.storage_mode == apprise.PersistentStoreMode.FLUSH

    def test_get_asset_hook_params_take_precedence_over_connection_extra(self, tmp_path):
        conn = Connection(
            conn_type="apprise",
            extra={"storage_path": str(tmp_path / "from_extra"), "storage_mode": "flush"},
        )
        hook = AppriseHook(storage_path=str(tmp_path / "from_param"), storage_mode="memory")

        asset = hook._get_asset(conn)

        assert asset is not None
        assert asset.storage_path == str(tmp_path / "from_param")
        assert asset.storage_mode == apprise.PersistentStoreMode.MEMORY

    def test_get_asset_without_connection_uses_hook_params(self, tmp_path):
        hook = AppriseHook(storage_path=str(tmp_path))

        asset = hook._get_asset(None)

        assert asset is not None
        assert asset.storage_path == str(tmp_path)
        assert asset.storage_mode == apprise.PersistentStoreMode.AUTO

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

    def test_get_asset_raises_on_invalid_storage_mode(self, tmp_path):
        conn = Connection(
            conn_type="apprise",
            extra={"storage_path": str(tmp_path), "storage_mode": "invalid"},
        )
        hook = AppriseHook()

        with pytest.raises(ValueError, match="Invalid Apprise storage_mode 'invalid'"):
            hook._get_asset(conn)

    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
    )
    def test_notify_without_storage_path_passes_no_asset(self, mock_conn):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={"config": {"path": "http://some_path_that_dont_exist/", "tag": "p0"}},
        )
        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise:
            hook = AppriseHook()
            hook.notify(body="test")

        assert mock_apprise.call_args.kwargs["asset"] is None

    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
    )
    def test_notify_passes_asset_from_connection_extra(self, mock_conn, tmp_path):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={
                "config": {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                "storage_path": str(tmp_path),
            },
        )
        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise:
            hook = AppriseHook()
            hook.notify(body="test")

        asset = mock_apprise.call_args.kwargs["asset"]
        assert asset.storage_path == str(tmp_path)

    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
    )
    def test_notify_with_explicit_config_does_not_fetch_connection(self, mock_conn, tmp_path):
        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        config = MagicMock(spec=apprise.AppriseConfig)
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise:
            hook = AppriseHook(storage_path=str(tmp_path))
            hook.notify(body="test", config=config)

        mock_conn.assert_not_called()
        apprise_obj.add.assert_called_once_with(config)
        asset = mock_apprise.call_args.kwargs["asset"]
        assert asset.storage_path == str(tmp_path)

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.get_async_connection",
    )
    async def test_async_notify_passes_asset_from_connection_extra(self, mock_conn, tmp_path):
        mock_conn.return_value = Connection(
            conn_id="apprise",
            extra={
                "config": {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                "storage_path": str(tmp_path),
            },
        )
        apprise_obj = apprise.Apprise()
        apprise_obj.async_notify = AsyncMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj) as mock_apprise:
            hook = AppriseHook()
            await hook.async_notify(body="test")

        asset = mock_apprise.call_args.kwargs["asset"]
        assert asset.storage_path == str(tmp_path)
