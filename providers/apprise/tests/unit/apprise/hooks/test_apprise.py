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
from unittest.mock import MagicMock, call, patch

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
        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=extra),
        ):
            hook = AppriseHook()
            assert hook.get_config_from_conn() == (json.loads(config) if isinstance(config, str) else config)

    def test_set_config_from_conn_with_dict(self):
        """
        Test set_config_from_conn for dict config
        """
        extra = {"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}}
        apprise_obj = apprise.Apprise()
        apprise_obj.add = MagicMock()
        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=extra),
        ):
            hook = AppriseHook()
            hook.set_config_from_conn(apprise_obj)

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
        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=extra),
        ):
            hook = AppriseHook()
            hook.set_config_from_conn(apprise_obj)

        apprise_obj.add.assert_has_calls(
            [
                call("http://some_path_that_dont_exist/", tag="p0"),
                call("http://some_other_path_that_dont_exist/", tag="p1"),
            ]
        )

    @mock.patch(
        "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
        return_value=Connection(
            conn_id="apprise",
            extra={
                "config": [
                    {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                    {"path": "http://some_other_path_that_dont_exist/", "tag": "p1"},
                ]
            },
        ),
    )
    def test_notify(self, connection):
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

    @pytest.mark.parametrize(
        "env_var_value,expected_path",
        [
            (None, "/tmp/apprise_cache"),  # Default behavior when env var is not set
            ("/custom/cache", "/custom/cache"),  # Custom path via environment variable
        ],
    )
    @mock.patch("airflow.providers.apprise.hooks.apprise.os.makedirs")
    @mock.patch("airflow.providers.apprise.hooks.apprise.AppriseAsset")
    @mock.patch("apprise.Apprise")
    def test_notify_persistent_storage(
        self, mock_apprise_cls, mock_asset_cls, mock_makedirs, monkeypatch, env_var_value, expected_path
    ):
        """
        Test that AppriseHook.notify instantiates Apprise with persistent storage enabled.
        Tests both default path and custom APPRISE_STORAGE_PATH environment variable.
        """
        mock_asset = mock.Mock()
        mock_asset_cls.return_value = mock_asset
        mock_apprise_obj = mock.Mock()
        mock_apprise_cls.return_value = mock_apprise_obj

        # Set or unset the environment variable
        if env_var_value is not None:
            monkeypatch.setenv("APPRISE_STORAGE_PATH", env_var_value)
        else:
            monkeypatch.delenv("APPRISE_STORAGE_PATH", raising=False)

        with mock.patch(
            "airflow.providers.apprise.hooks.apprise.AppriseHook.get_connection",
            return_value=Connection(
                conn_id="apprise",
                extra={
                    "config": [
                        {"path": "http://some_path_that_dont_exist/", "tag": "p0"},
                    ]
                },
            ),
        ):
            hook = AppriseHook()
            hook.notify(body="test")

        # Ensure makedirs was called with the expected path
        mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)

        # Ensure AppriseAsset was created with correct arguments
        assert mock_asset_cls.call_count == 1
        asset_args, asset_kwargs = mock_asset_cls.call_args
        assert "storage_path" in asset_kwargs
        assert asset_kwargs["storage_path"] == expected_path
        assert "storage_mode" in asset_kwargs

        # Ensure Apprise was instantiated with the asset
        mock_apprise_cls.assert_called_once_with(asset=mock_asset)
