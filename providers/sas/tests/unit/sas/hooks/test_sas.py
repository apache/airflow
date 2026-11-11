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
"""Tests for ``SASHook``: connection settings, the DataFrame API and the async methods."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, create_autospec, patch

import pandas as pd
import pytest

from airflow.providers.sas.driver.metadata import SASMetadata
from airflow.providers.sas.driver.script import SASSettings
from airflow.providers.sas.hooks.sas import SASHook

from unit.sas.fakes import (
    _ED25519_HOST_KEY,
    COLUMN_FORMATS,
    COLUMN_TYPES,
    DATE32,
    _async_connection,
    _async_ssh,
    _connect,
    _hook_with_frame,
    _live_connection,
    _patch_async_lookup,
    _raw_frame,
    _sas_connection,
)


def test_get_df_returns_typed_dataframe_from_one_sas_run():
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    hook, cursor = _hook_with_frame(typed)

    df = hook.get_df("select * from mylib.orders", limit=10)

    cursor.fetch_dataframe.assert_called_once_with(
        "select * from mylib.orders", None, limit=10, dtype_backend=None
    )
    assert df["DATDEP"].dtype == DATE32


def test_get_df_by_chunks_streams_from_one_sas_run():
    hook, cursor = _hook_with_frame(pd.DataFrame())
    cursor.fetch_dataframe_chunks.return_value = iter(
        [pd.DataFrame({"A": [1.0, 2.0]}), pd.DataFrame({"A": [3.0]})]
    )

    chunks = list(
        hook.get_df_by_chunks(
            "select * from mylib.orders",
            chunksize=2,
            limit=3,
            dtype_backend="pyarrow",
        )
    )

    assert [len(c) for c in chunks] == [2, 1]
    cursor.fetch_dataframe_chunks.assert_called_once_with(
        "select * from mylib.orders",
        None,
        chunksize=2,
        limit=3,
        dtype_backend="pyarrow",
    )
    cursor.fetch_dataframe.assert_not_called()


def test_get_df_passes_dtype_backend_to_the_cursor():
    typed = SASMetadata().normalize_dataframe(
        _raw_frame(), column_types=COLUMN_TYPES, column_formats=COLUMN_FORMATS
    )
    hook, cursor = _hook_with_frame(typed)

    hook.get_df("select * from mylib.orders", dtype_backend="pyarrow")

    assert cursor.fetch_dataframe.call_args.kwargs["dtype_backend"] == "pyarrow"


def test_get_conn_constructor_arguments_win_over_extra():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection(
            {
                "sas_binary": "/from/extra",
                "encoding": "utf-8",
                "sas_temp_dir": "/extra/tmp",
            }
        ),
        sas_binary="/from/ctor",
        encoding="latin-1",
    )
    conn, _ = _connect(hook)

    assert conn.settings.sas_binary == "/from/ctor"
    assert conn.settings.encoding == "latin-1"
    assert conn.settings.sas_temp_dir == "/extra/tmp", "extra applies when the constructor is silent"


def test_get_conn_falls_back_to_defaults_and_lowercases_libraries():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"sas_libraries": {"MYLIB": "/data/mylib"}}),
    )
    conn, ssh_hook_cls = _connect(hook)

    defaults = SASSettings()
    assert conn.settings.sas_binary == defaults.sas_binary
    assert conn.settings.sas_options == defaults.sas_options
    assert conn.settings.sas_temp_dir == defaults.sas_temp_dir
    assert conn.settings.sas_libraries == {"mylib": "/data/mylib"}
    assert ssh_hook_cls.call_args.kwargs["keepalive_interval"] == 30


@pytest.mark.parametrize(
    ("ctor", "extra", "expected"),
    [
        (None, {"keepalive_interval": 5}, 5),
        (10, {"keepalive_interval": 5}, 10),
        (0, {"keepalive_interval": 5}, 0),
        (None, {"keepalive_interval": 0}, 0),
    ],
)
def test_get_conn_keepalive_precedence_and_zero_disables(ctor, extra, expected):
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection(extra),
        keepalive_interval=ctor,
    )
    _, ssh_hook_cls = _connect(hook)

    assert ssh_hook_cls.call_args.kwargs["keepalive_interval"] == expected


def test_get_conn_host_key_extra_pins_the_server_key():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"host_key": _ED25519_HOST_KEY}),
    )
    _, ssh_hook_cls = _connect(hook)

    ssh_hook = ssh_hook_cls.return_value
    assert ssh_hook.host_key.get_name() == "ssh-ed25519"
    assert ssh_hook.host_key.get_base64() == _ED25519_HOST_KEY.split()[1]
    assert ssh_hook.no_host_key_check is False


@pytest.mark.parametrize(("value", "expected"), [("true", True), (False, False)])
def test_get_conn_no_host_key_check_extra_is_applied(value, expected):
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"no_host_key_check": value}),
    )
    _, ssh_hook_cls = _connect(hook)

    assert ssh_hook_cls.return_value.no_host_key_check is expected


def test_get_conn_without_host_key_extras_keeps_the_ssh_provider_default():
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())
    _, ssh_hook_cls = _connect(hook)

    ssh_hook = ssh_hook_cls.return_value
    assert "no_host_key_check" not in vars(ssh_hook)
    assert "host_key" not in vars(ssh_hook)


def test_get_conn_host_key_and_no_host_key_check_conflict():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"host_key": _ED25519_HOST_KEY, "no_host_key_check": "true"}),
    )

    with pytest.raises(ValueError, match="Must check host key"):
        _connect(hook)


def test_get_conn_delegated_mode_reads_the_sas_extras():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"sas_binary": "/extra/sas", "sas_temp_dir": "/extra/tmp"}),
        ssh_conn_id="ssh.sas",
        sas_binary="/ctor/sas",
    )
    conn, ssh_hook_cls = _connect(hook)

    ssh_hook_cls.assert_called_once_with(ssh_conn_id="ssh.sas")
    assert conn.settings.sas_binary == "/ctor/sas", "the constructor still wins"
    assert conn.settings.sas_temp_dir == "/extra/tmp"


def test_get_conn_direct_mode_without_host_says_what_is_missing():
    from airflow.models import Connection

    hook = SASHook(sas_conn_id="sas.test", connection=Connection(conn_id="sas.test", conn_type="sas"))

    with pytest.raises(ValueError, match="has no host"):
        _connect(hook)


def test_get_conn_delegated_mode_from_the_ssh_conn_id_extra():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"ssh_conn_id": "ssh.from_extra"}),
    )
    _, ssh_hook_cls = _connect(hook)

    ssh_hook_cls.assert_called_once_with(ssh_conn_id="ssh.from_extra")


def test_get_conn_ssh_conn_id_argument_wins_over_extra():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"ssh_conn_id": "ssh.from_extra"}),
        ssh_conn_id="ssh.from_ctor",
    )
    _, ssh_hook_cls = _connect(hook)

    ssh_hook_cls.assert_called_once_with(ssh_conn_id="ssh.from_ctor")


def test_get_conn_reuses_an_active_connection():
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())
    cached = _live_connection()
    hook._cached_conn = cached

    conn, ssh_hook_cls = _connect(hook)

    assert conn is cached
    ssh_hook_cls.assert_not_called()


@pytest.mark.parametrize(
    "state",
    [{"transport_active": False}, {"has_transport": False}, {"sftp_closed": True}],
)
def test_get_conn_replaces_a_closed_connection(state):
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())
    hook._log = create_autospec(logging.Logger, instance=True)
    dead = _live_connection(**state)
    dead._sftp.close.side_effect = EOFError("channel gone")
    hook._cached_conn = dead

    conn, ssh_hook_cls = _connect(hook)

    assert conn is not dead
    assert hook._cached_conn is conn
    ssh_hook_cls.assert_called_once()
    dead._ssh.close.assert_called_once()
    hook._log.warning.assert_called_once()


def test_dialect_is_cached_per_hook():
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())

    assert hook.dialect is hook.dialect


def test_hook_timeout_precedence_and_default_options_include_nonews():
    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"timeout": "120"}),
        timeout=30,
    )
    conn, _ = _connect(hook)
    assert conn.settings.timeout == 30.0
    assert "-nonews" in conn.settings.sas_options

    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection({"timeout": "120"}))
    conn, _ = _connect(hook)
    assert conn.settings.timeout == 120.0


def test_hook_compress_defaults_and_validation():
    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection())
    conn, ssh_hook_cls = _connect(hook)
    assert conn.settings.sas_compress == "yes"
    assert ssh_hook_cls.return_value.compress is True, "zlib on by default"

    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"sas_compress": "no", "ssh_compress": "false"}),
    )
    conn, ssh_hook_cls = _connect(hook)
    assert conn.settings.sas_compress == "no"
    assert ssh_hook_cls.return_value.compress is False

    hook = SASHook(
        sas_conn_id="sas.test",
        connection=_sas_connection({"sas_compress": "no"}),
        sas_compress="BINARY",
    )
    conn, _ = _connect(hook)
    assert conn.settings.sas_compress == "binary"

    hook = SASHook(sas_conn_id="sas.test", connection=_sas_connection({"sas_compress": "zip"}))
    with pytest.raises(ValueError, match="sas_compress"):
        _connect(hook)


@pytest.mark.parametrize(
    ("hook_kwargs", "extra", "ssh_conn_id", "compress"),
    [
        ({}, {"sas_temp_dir": "/extra/tmp"}, "sas.test", True),
        ({}, {"ssh_compress": "false"}, "sas.test", False),
        ({}, {"ssh_conn_id": "ssh.sas"}, "ssh.sas", None),
        ({"ssh_conn_id": "ssh.ctor"}, {}, "ssh.ctor", None),
    ],
)
@pytest.mark.asyncio
async def test_aget_conn_reads_everything_asynchronously(hook_kwargs, extra, ssh_conn_id, compress):
    import asyncssh

    from airflow.providers.ssh.hooks.ssh import SSHHookAsync

    ssh = _async_ssh()
    # start_sftp_client is awaitable and an async context manager in asyncssh,
    # not a plain ``async def``, so autospec cannot make it awaitable.
    ssh.start_sftp_client = AsyncMock(return_value=create_autospec(asyncssh.SFTPClient, instance=True))
    hook = SASHook(sas_conn_id="sas.test", **hook_kwargs)
    patches = _patch_async_lookup(extra, ssh)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as init:
        conn = await hook.aget_conn()
        again = await hook.aget_conn()

    assert again is conn, "cached while active"
    init.assert_called_once()
    assert init.call_args.kwargs["ssh_conn_id"] == ssh_conn_id
    ssh_hook = init.call_args.args[0]
    assert getattr(ssh_hook, "compress", None) is compress
    if "sas_temp_dir" in extra:
        assert conn.settings.sas_temp_dir == "/extra/tmp"
    assert isinstance(ssh_hook, SSHHookAsync)


@pytest.mark.asyncio
async def test_aget_conn_replaces_a_closed_connection():
    import asyncssh

    ssh = _async_ssh()
    # start_sftp_client is awaitable and an async context manager in asyncssh,
    # not a plain ``async def``, so autospec cannot make it awaitable.
    ssh.start_sftp_client = AsyncMock(return_value=create_autospec(asyncssh.SFTPClient, instance=True))
    hook = SASHook(sas_conn_id="sas.test")
    hook._log = create_autospec(logging.Logger, instance=True)
    dead, dead_ssh, _ = _async_connection()
    dead_ssh.is_closed.return_value = True
    hook._cached_async_conn = dead
    patches = _patch_async_lookup({}, ssh)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
        conn = await hook.aget_conn()

    assert conn is not dead
    dead_ssh.close.assert_called_once()
    hook._log.warning.assert_called_once()


@pytest.mark.asyncio
async def test_aget_df_and_aget_records_go_through_the_async_connection():
    from airflow.providers.sas.driver.connection import SASAsyncConnection

    hook = SASHook(sas_conn_id="sas.test")
    conn = create_autospec(SASAsyncConnection, instance=True)
    conn.fetch_dataframe.return_value = pd.DataFrame({"A": [1.0]})
    conn.fetch_records.return_value = [(1.0,)]
    with patch.object(SASHook, "aget_conn", AsyncMock(return_value=conn)):
        df = await hook.aget_df("select 1 from mylib.x", limit=3, dtype_backend="pyarrow")
        rows = await hook.aget_records(["ignored", "select 1 from mylib.x"], {"a": 1})

    assert df["A"].tolist() == [1.0]
    conn.fetch_dataframe.assert_awaited_once_with(
        "select 1 from mylib.x", None, limit=3, dtype_backend="pyarrow"
    )
    assert rows == [(1.0,)]
    conn.fetch_records.assert_awaited_once_with("select 1 from mylib.x", {"a": 1})


@pytest.mark.asyncio
async def test_aget_df_by_chunks_streams_from_the_async_connection():
    from airflow.providers.sas.driver.connection import SASAsyncConnection

    async def two_chunks(*args, **kwargs):
        yield pd.DataFrame({"A": [1.0]})
        yield pd.DataFrame({"A": [2.0]})

    hook = SASHook(sas_conn_id="sas.test")
    conn = create_autospec(SASAsyncConnection, instance=True)
    conn.fetch_dataframe_chunks.side_effect = two_chunks
    with patch.object(SASHook, "aget_conn", AsyncMock(return_value=conn)):
        chunks = [
            chunk async for chunk in hook.aget_df_by_chunks("select * from mylib.x", chunksize=1, limit=2)
        ]

    assert [c["A"].tolist() for c in chunks] == [[1.0], [2.0]]
    conn.fetch_dataframe_chunks.assert_called_once_with(
        "select * from mylib.x", None, chunksize=1, limit=2, dtype_backend=None
    )


def test_close_closes_the_cached_connection_once():
    hook = SASHook(sas_conn_id="sas.test")
    conn = _live_connection()
    hook._cached_conn = conn

    hook.close()
    hook.close()

    conn._ssh.close.assert_called_once()
    assert hook._cached_conn is None


@pytest.mark.asyncio
async def test_aclose_closes_the_cached_async_connection_once():
    hook = SASHook(sas_conn_id="sas.test")
    conn, ssh, sftp = _async_connection()
    hook._cached_async_conn = conn

    await hook.aclose()
    await hook.aclose()

    assert sftp.exited
    ssh.close.assert_called_once()
    ssh.wait_closed.assert_awaited_once()
    assert hook._cached_async_conn is None
