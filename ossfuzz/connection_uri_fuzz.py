#!/usr/bin/python3
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

import os
import sys
from urllib.parse import urlencode

import atheris

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

_CONN_TYPES = [
    "postgres",
    "postgresql",
    "mysql",
    "mssql",
    "sqlite",
    "http",
    "https",
    "ssh",
    "s3",
    "gcs",
    "wasb",
]
_HOST_PROTOCOLS = ["http", "https", "tcp", "udp"]


def _maybe_consume_str(fdp: atheris.FuzzedDataProvider, max_len: int) -> str | None:
    if not fdp.ConsumeBool():
        return None
    return fdp.ConsumeString(max_len)


def _build_query(fdp: atheris.FuzzedDataProvider) -> str:
    params: dict[str, str] = {}
    for _ in range(fdp.ConsumeIntInRange(0, 8)):
        k = fdp.ConsumeString(16)
        v = fdp.ConsumeString(64)
        if k:
            params[k] = v
    if not params:
        return ""
    return "?" + urlencode(params, doseq=False)


def _build_uri(fdp: atheris.FuzzedDataProvider) -> str:
    conn_type = fdp.PickValueInList(_CONN_TYPES)
    host_protocol = fdp.PickValueInList(_HOST_PROTOCOLS) if fdp.ConsumeBool() else None

    login = _maybe_consume_str(fdp, 32) or ""
    password = _maybe_consume_str(fdp, 32) or ""
    host = _maybe_consume_str(fdp, 64) or ""
    schema = _maybe_consume_str(fdp, 64) or ""
    port = fdp.ConsumeIntInRange(0, 65535) if fdp.ConsumeBool() else None

    authority = ""
    if login or password:
        authority = login
        if password or fdp.ConsumeBool():
            authority += ":" + password
        authority += "@"

    host_block = host
    if port is not None and (host_block or authority):
        host_block += f":{port}"
    if schema:
        host_block += f"/{schema}"

    uri = f"{conn_type}://"
    if host_protocol:
        uri += f"{host_protocol}://"
    uri += authority + host_block
    uri += _build_query(fdp)
    return uri


with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    from airflow.exceptions import AirflowException
    from airflow.models.connection import Connection, sanitize_conn_id


def TestInput(input_bytes: bytes):
    fdp = atheris.FuzzedDataProvider(input_bytes)

    conn_id = fdp.ConsumeString(256)
    _ = sanitize_conn_id(conn_id)

    uri = _build_uri(fdp)
    try:
        conn = Connection(conn_id=conn_id, uri=uri)
        _ = conn.get_uri()
        _ = conn.extra_dejson
    except (AirflowException, ValueError, TypeError, UnicodeError):
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

