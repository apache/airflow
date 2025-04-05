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

import json
import logging
from json import JSONDecodeError
from typing import Any

import attrs

from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)


@attrs.define
class Connection:
    """
    A connection to an external data source.

    :param conn_id: The connection ID.
    :param conn_type: The connection type.
    :param description: The connection description.
    :param host: The host.
    :param login: The login.
    :param password: The password.
    :param schema: The schema.
    :param port: The port number.
    :param extra: Extra metadata. Non-standard data such as private/SSH keys can be saved here. JSON
        encoded object.
    """

    conn_id: str
    conn_type: str
    description: str | None = None
    host: str | None = None
    schema: str | None = None
    login: str | None = None
    password: str | None = None
    port: int | None = None
    extra: str | None = None

    EXTRA_KEY = "__extra__"

    def get_uri(self) -> str:
        """Generate and return connection in URI format."""
        from urllib.parse import parse_qsl, quote, urlencode

        if self.conn_type and "_" in self.conn_type:
            log.warning(
                "Connection schemes (type: %s) shall not contain '_' according to RFC3986.",
                self.conn_type,
            )
        if self.conn_type:
            uri = f"{self.conn_type.lower().replace('_', '-')}://"
        else:
            uri = "//"

        if self.host and "://" in self.host:
            protocol, host = self.host.split("://", 1)
        else:
            protocol, host = None, self.host or ""
        if protocol:
            uri += f"{protocol}://"

        authority_block = ""
        if self.login is not None:
            authority_block += quote(self.login, safe="")
        if self.password is not None:
            authority_block += ":" + quote(self.password, safe="")
        if authority_block > "":
            authority_block += "@"
            uri += authority_block

        host_block = ""
        if host != "":
            host_block += quote(host, safe="")
        if self.port:
            if host_block == "" and authority_block == "":
                host_block += f"@:{self.port}"
            else:
                host_block += f":{self.port}"
        if self.schema:
            host_block += f"/{quote(self.schema, safe='')}"
        uri += host_block

        if self.extra:
            try:
                query: str | None = urlencode(self.extra_dejson)
            except TypeError:
                query = None
            if query and self.extra_dejson == dict(parse_qsl(query, keep_blank_values=True)):
                uri += ("?" if self.schema else "/?") + query
            else:
                uri += ("?" if self.schema else "/?") + urlencode({self.EXTRA_KEY: self.extra})

        return uri

    def get_hook(self, *, hook_params=None):
        """Return hook based on conn_type."""
        from airflow.providers_manager import ProvidersManager
        from airflow.utils.module_loading import import_string

        hook = ProvidersManager().hooks.get(self.conn_type, None)

        if hook is None:
            raise AirflowException(f'Unknown hook type "{self.conn_type}"')
        try:
            hook_class = import_string(hook.hook_class_name)
        except ImportError:
            log.error(
                "Could not import %s when discovering %s %s",
                hook.hook_class_name,
                hook.hook_name,
                hook.package_name,
            )
            raise
        if hook_params is None:
            hook_params = {}
        return hook_class(**{hook.connection_id_attribute_name: self.conn_id}, **hook_params)

    @classmethod
    def get(cls, conn_id: str) -> Any:
        from airflow.sdk.execution_time.context import _get_connection

        return _get_connection(conn_id)

    @property
    def extra_dejson(self) -> dict:
        """Deserialize `extra` property to JSON."""
        extra = {}
        if self.extra:
            try:
                extra = json.loads(self.extra)
            except JSONDecodeError:
                log.exception("Failed to deserialize extra property `extra`, returning empty dictionary")
        # TODO: Mask sensitive keys from this list or revisit if it will be done in server
        return extra
