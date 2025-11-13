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

from inspect import getfullargspec
from unittest import mock

import pytest

from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.providers.samba.hooks.samba import SambaHook

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"
PATH_PARAMETER_NAMES = {"path", "src", "dst"}


class TestSambaHook:
    @pytest.mark.db_test
    def test_get_conn_should_fail_if_conn_id_does_not_exist(self, sdk_connection_not_found):
        with pytest.raises(AirflowNotFoundException):
            SambaHook("non-existed-connection-id")

    @mock.patch("smbclient.register_session")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_context_manager(self, get_conn_mock, register_session):
        CONNECTION = Connection(
            host="ip",
            schema="share",
            login="username",
            password="password",
        )
        get_conn_mock.return_value = CONNECTION
        register_session.return_value = None
        with SambaHook("samba_default"):
            args, kwargs = tuple(register_session.call_args_list[0])
            assert args == (CONNECTION.host,)
            assert kwargs == {
                "username": CONNECTION.login,
                "password": CONNECTION.password,
                "port": 445,
                "connection_cache": {},
            }
            cache = kwargs.get("connection_cache")
            mock_connection = mock.Mock()
            mock_connection.disconnect.return_value = None
            cache["foo"] = mock_connection

        # Test that the connection was disconnected upon exit.
        mock_connection.disconnect.assert_called_once()

    @pytest.mark.parametrize(
        "name",
        [
            "getxattr",
            "link",
            "listdir",
            "listxattr",
            "lstat",
            "makedirs",
            "mkdir",
            "open_file",
            "readlink",
            "remove",
            "removedirs",
            "removexattr",
            "rename",
            "replace",
            "rmdir",
            "scandir",
            "setxattr",
            "stat",
            "stat_volume",
            "symlink",
            "truncate",
            "unlink",
            "utime",
            "walk",
        ],
    )
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_method(self, get_conn_mock, name):
        CONNECTION = Connection(
            host="ip",
            schema="share",
            login="username",
            password="password",
        )

        get_conn_mock.return_value = CONNECTION
        hook = SambaHook("samba_default")
        connection_settings = {
            "connection_cache": {},
            "username": CONNECTION.login,
            "password": CONNECTION.password,
            "port": 445,
        }
        with mock.patch("smbclient." + name) as p:
            kwargs = {}
            method = getattr(hook, name)
            spec = getfullargspec(method)

            if spec.defaults:
                for default in reversed(spec.defaults):
                    arg = spec.args.pop()
                    kwargs[arg] = default

            # Ignore "self" argument.
            args = spec.args[1:]

            method(*args, **kwargs)
            assert len(p.mock_calls) == 1

            # Verify positional arguments. If the argument is a path parameter, then we expect
            # the hook implementation to fully qualify the path.
            p_args, p_kwargs = tuple(p.call_args_list[0])
            for arg, provided in zip(args, p_args):
                if arg in PATH_PARAMETER_NAMES:
                    expected = "//" + CONNECTION.host + "/" + CONNECTION.schema + "/" + arg
                else:
                    expected = arg
                assert expected == provided

            # We expect keyword arguments to include the connection settings.
            assert dict(kwargs, **connection_settings) == p_kwargs

    @pytest.mark.parametrize(
        ("path", "path_type", "full_path"),
        [
            # Linux path -> Linux path, no path_type (default)
            ("/start/path/with/slash", None, "//ip/share/start/path/with/slash"),
            ("start/path/without/slash", None, "//ip/share/start/path/without/slash"),
            # Linux path -> Linux path, explicit path_type (posix)
            ("/start/path/with/slash/posix", "posix", "//ip/share/start/path/with/slash/posix"),
            ("start/path/without/slash/posix", "posix", "//ip/share/start/path/without/slash/posix"),
            # Linux path -> Windows path, explicit path_type (windows)
            ("/start/path/with/slash/windows", "windows", r"\\ip\share\start\path\with\slash\windows"),
            ("start/path/without/slash/windows", "windows", r"\\ip\share\start\path\without\slash\windows"),
            # Windows path -> Windows path, explicit path_type (windows)
            (
                r"\start\path\with\backslash\windows",
                "windows",
                r"\\ip\share\start\path\with\backslash\windows",
            ),
            (
                r"start\path\without\backslash\windows",
                "windows",
                r"\\ip\share\start\path\without\backslash\windows",
            ),
        ],
    )
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test__join_path(
        self,
        get_conn_mock,
        path,
        path_type,
        full_path,
    ):
        CONNECTION = Connection(
            host="ip",
            schema="share",
            login="username",
            password="password",
        )

        get_conn_mock.return_value = CONNECTION
        hook = SambaHook("samba_default", share_type=path_type)
        assert hook._join_path(path) == full_path

    @mock.patch("airflow.providers.samba.hooks.samba.smbclient.open_file", return_value=mock.Mock())
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_open_file(self, get_conn_mock, open_file_mock):
        CONNECTION = Connection(
            host="ip",
            schema="share",
            login="username",
            password="password",
        )

        get_conn_mock.return_value = CONNECTION
        samba_hook = SambaHook("samba_default")
        path = "test_file.txt"
        mode = "wb"
        result = samba_hook.open_file(path, mode=mode)
        assert result is not None, "open_file method returned None"
        assert hasattr(result, "write"), f"Error: {result} does not have a 'write' method"
