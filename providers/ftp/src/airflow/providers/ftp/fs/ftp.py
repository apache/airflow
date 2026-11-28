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

import ftplib
from typing import TYPE_CHECKING, Any

from fsspec.implementations.ftp import FTPFileSystem

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.ftp.hooks.ftp import _get_passive_mode

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["ftp"]


class _AirflowFTPFileSystem(FTPFileSystem):
    """
    ``FTPFileSystem`` that honors the ``passive`` extra and survives a closed control connection.

    fsspec has no option for passive mode, so it is applied on every ``_connect``. The filesystem
    is cached for the life of the process, while fsspec reconnects only after a failed read, and
    ``ls`` on a connection the server has closed returns an empty list instead of failing. So
    ``ls`` and ``_open`` check the connection first and reconnect when it is gone.
    """

    def __init__(self, *args: Any, passive: bool = True, **kwargs: Any) -> None:
        self.passive = passive
        super().__init__(*args, **kwargs)

    def _connect(self) -> None:
        super()._connect()
        self.ftp.set_pasv(self.passive)

    def _ensure_connected(self) -> None:
        try:
            self.ftp.voidcmd("NOOP")
        except ftplib.all_errors:
            self._connect()

    def ls(self, path, detail=True, **kwargs):
        self._ensure_connected()
        return super().ls(path, detail=detail, **kwargs)

    def _open(self, path, mode="rb", **kwargs):
        self._ensure_connected()
        return super()._open(path, mode=mode, **kwargs)


def get_fs(conn_id: str | None, storage_options: dict[str, Any] | None = None) -> AbstractFileSystem:
    if conn_id is None:
        manual_options = storage_options or {}
        if not manual_options.get("host"):
            raise ValueError("An FTP path needs a connection id, for example ftp://my_conn@/path/to/file.")
        return _AirflowFTPFileSystem(**manual_options)

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
    if not options["host"]:
        raise ValueError(f"FTP connection {conn_id!r} has no host to connect to.")
    return _AirflowFTPFileSystem(passive=_get_passive_mode(extras), **options)
