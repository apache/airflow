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

from functools import cached_property

import requests

from airflow.hooks.base import BaseHook


class DremioException(Exception):
    """Dremio Exception class."""


def _trim_trailing_slash(url: str) -> str:
    if url.endswith("/"):
        return url[:-1]
    return url


class DremioHook(BaseHook):
    """Interacts with Dremio Cluster using Dremio REST APIs."""

    conn_attr_name = "dremio_conn_id"
    default_conn_name = "dremio_default"
    conn_type = "http"
    hook_name = "Dremio"

    def __init__(self, dremio_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        self.dremio_conn_id = dremio_conn_id
        super().__init__(*args, **kwargs)

    @cached_property
    def url(self) -> str:
        conn = self.get_connection(conn_id=self.dremio_conn_id)
        base_url = ""
        host = _trim_trailing_slash(conn.host) if conn.host else ""
        if "://" in host:
            base_url = base_url + host
        else:
            schema = conn.schema if conn.schema else "http"
            base_url = f"{schema}://{host}"

        if conn.port:
            base_url = f"{base_url}:{conn.port}"

        return base_url

    def get_conn(self) -> requests.Session:
        session = requests.Session()
        conn = self.get_connection(conn_id=self.dremio_conn_id)

        host = conn.host if conn.host else ""
        if not host:
            raise DremioException(
                f"The connection {self.dremio_conn_id} does not have the dremio hostname set. "
                f"Please give a valid hostname in the airflow connection"
            )

        return session
