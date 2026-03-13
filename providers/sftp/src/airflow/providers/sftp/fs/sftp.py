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

from airflow.sdk.bases.hook import BaseHook

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

schemes = ["sftp", "ssh"]


def get_fs(conn_id: str | None, storage_options: dict[str, Any] | None = None) -> AbstractFileSystem:
    try:
        from sshfs import SSHFileSystem
    except ImportError:
        raise ImportError(
            "Airflow FS SFTP/SSH protocol requires the sshfs library. "
            "Install with: pip install apache-airflow-providers-sftp[sshfs]"
        )

    if conn_id is None:
        return SSHFileSystem(**(storage_options or {}))

    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson

    options: dict[str, Any] = {
        "host": conn.host,
        "port": conn.port or 22,
        "username": conn.login,
    }

    if conn.password:
        options["password"] = conn.password

    if key_file := extras.get("key_file"):
        options["client_keys"] = [key_file]

    if private_key := extras.get("private_key"):
        options["client_keys"] = [private_key]
        if passphrase := extras.get("private_key_passphrase"):
            options["passphrase"] = passphrase

    if str(extras.get("no_host_key_check", "")).lower() == "true":
        options["known_hosts"] = None

    options.update(storage_options or {})
    return SSHFileSystem(**options)
