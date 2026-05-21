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

from collections.abc import Generator
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from asyncssh import SFTPClient, SSHClientConnection

pytest_plugins = "tests_common.pytest_plugin"

if TYPE_CHECKING:
    from airflow.providers.sftp.hooks.sftp import SFTPHookAsync


@pytest.fixture
def sftp_hook_mocked() -> Generator[tuple[SFTPHookAsync, SFTPClient], Any, None]:
    """
    Fixture that mocks SFTPHookAsync._get_conn with SSH + SFTP async mocks.
    Returns a tuple (hook, sftp_client_mock) so tests can easily set readdir.
    """
    from airflow.providers.sftp.hooks.sftp import SFTPHookAsync

    sftp_client_mock = AsyncMock(spec=SFTPClient)
    sftp_client_mock.readdir.return_value = []
    # asyncssh's SFTPClient.open() is decorated to be both awaitable and async-context-
    # manageable; the production code uses ``async with sftp.open(...) as remote_file``,
    # so the mock must return a context manager from a *synchronous* call. The spec
    # auto-makes it an AsyncMock (call → coroutine); override it with a sync MagicMock.
    sftp_client_mock.open = MagicMock()

    # asyncssh's start_sftp_client() has the same shape — production uses it as
    # ``async with ssh_conn.start_sftp_client() as sftp:``. Same override pattern.
    sftp_cm_mock = MagicMock()
    sftp_cm_mock.__aenter__ = AsyncMock(return_value=sftp_client_mock)
    sftp_cm_mock.__aexit__ = AsyncMock(return_value=None)

    client_connection_mock = AsyncMock(spec=SSHClientConnection)
    client_connection_mock.start_sftp_client = MagicMock(return_value=sftp_cm_mock)

    with patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn") as mock_get_conn:
        mock_get_conn.return_value.__aenter__.return_value = client_connection_mock

        yield SFTPHookAsync(), sftp_cm_mock
