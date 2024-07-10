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

PATH_PARAMETER_NAMES = {"path", "src", "dst"}

CONNECTION = Connection(
    host="ip",
    schema="share",
    login="username",
    password="password",
)


class TestSambaHook:
    @pytest.mark.db_test
    def test_get_conn_should_fail_if_conn_id_does_not_exist(self):
        with pytest.raises(AirflowNotFoundException):
            SambaHook("non-existed-connection-id")

    @mock.patch("smbclient.register_session")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_context_manager(self, get_conn_mock, register_session):
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
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_method(self, get_conn_mock, name):
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
        "path, full_path",
        [
            ("/start/path/with/slash", "//ip/share/start/path/with/slash"),
            ("start/path/without/slash", "//ip/share/start/path/without/slash"),
        ],
    )
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test__join_path(self, get_conn_mock, path, full_path):
        get_conn_mock.return_value = CONNECTION
        hook = SambaHook("samba_default")
        assert hook._join_path(path) == full_path
