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

from unittest.mock import MagicMock, call, patch

import apprise

from airflow.models import Connection
from airflow.providers.apprise.hooks.apprise import AppriseHook


class TestAppriseHook:
    """
    Test for AppriseHook
    """

    def test_get_config_from_conn(self):
        extra = {"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}}
        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra=extra),
        ):
            hook = AppriseHook()
            assert hook.get_config_from_conn() == extra['config']

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
        extra = {"config": {
            [{"path": "http://some_path_that_dont_exist/", "tag": "p0"},
             {"path": "http://some_other_path_that_dont_exist/", "tag": "p1"}]
        }}

        apprise_obj = apprise.Apprise()
        apprise_obj.add = MagicMock()
        with patch.object(
            AppriseHook,
            "get_connection",
            return_value=Connection(conn_type="apprise", extra={"config": extra}),
        ):
            hook = AppriseHook()
            hook.set_config_from_conn(apprise_obj)
            apprise_obj.add.assert_has_calls(
                [
                    call("http://some_path_that_dont_exist/", tag="p0"),
                    call("http://some_other_path_that_dont_exist/", tag="p1"),
                ]
            )

    def test_notify(self):
        apprise_obj = apprise.Apprise()
        apprise_obj.notify = MagicMock()
        apprise_obj.add = MagicMock()
        with patch.object(apprise, "Apprise", return_value=apprise_obj):
            hook = AppriseHook()
            hook.notify(
                body="test",
                config=True,
            )
            apprise_obj.notify.assert_called_once_with(
                body="test",
                title="",
                notify_type="info",
                body_format="text",
                tag=None,
                attach=None,
                interpret_escapes=None,
            )
