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

from typing import TYPE_CHECKING, Any

from fsspec.implementations.ftp import FTPFileSystem

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.ftp.hooks.ftp import _get_passive_mode

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["ftp"]


class _ActiveModeFTPFileSystem(FTPFileSystem):
    """
    ``FTPFileSystem`` that turns passive mode off on every connection.

    fsspec has no option for it and keeps the ``ftplib`` default (passive), while ``FTPHook``
    honors the ``passive`` connection extra. ``_connect`` also runs when a read reconnects.
    """

    def _connect(self) -> None:
        super()._connect()
        self.ftp.set_pasv(False)


def get_fs(conn_id: str | None, storage_options: dict[str, Any] | None = None) -> AbstractFileSystem:
    if conn_id is None:
        return FTPFileSystem(**(storage_options or {}))

    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson

    options: dict[str, Any] = {
        "host": conn.host,
        "port": conn.port or 21,
        "username": conn.login,
        "password": conn.password,
    }
    if encoding := extras.get("encoding"):
        options["encoding"] = encoding

    options.update(storage_options or {})
    if _get_passive_mode(extras):
        return FTPFileSystem(**options)
    return _ActiveModeFTPFileSystem(**options)
