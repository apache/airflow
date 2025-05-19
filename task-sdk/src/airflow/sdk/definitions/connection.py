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
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

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

    @staticmethod
    def _normalize_conn_type(conn_type: str | None) -> str | None:
        """Normalize conn_type."""
        if conn_type is None:
            return None
        conn_type = conn_type.lower()
        return conn_type

    @staticmethod
    def _parse_netloc_to_hostname(uri_parts) -> str:
        """
        Parse a URI string to get the correct Hostname.

        Returns hostname from netloc, handling special cases.
        """
        hostname = unquote(uri_parts.hostname or "")
        if "/" in hostname:
            hostname = uri_parts.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = unquote(hostname)
        return hostname

    @staticmethod
    def _create_host(protocol: str | None, host: str | None) -> str | None:
        """Return the connection host with the protocol."""
        if not host:
            return host
        if protocol:
            return f"{protocol}://{host}"
        return host

    def parse_from_uri(self, uri: str) -> None:
        """
        Parse connection parameters from a URI string.

        :param uri: The URI string to parse.
        """
        schemes_count_in_uri = uri.count("://")
        if schemes_count_in_uri > 2:
            raise AirflowException(f"Invalid connection string: {uri}.")
        host_with_protocol = schemes_count_in_uri == 2
        uri_parts = urlsplit(uri)
        conn_type = uri_parts.scheme
        normalized_conn_type = self._normalize_conn_type(conn_type)
        # Ensure we have a string value for conn_type (not None)
        self.conn_type = normalized_conn_type or ""
        rest_of_the_url = uri.replace(f"{conn_type}://", ("" if host_with_protocol else "//"))
        if host_with_protocol:
            uri_splits = rest_of_the_url.split("://", 1)
            if "@" in uri_splits[0] or ":" in uri_splits[0]:
                raise AirflowException(f"Invalid connection string: {uri}.")
        uri_parts = urlsplit(rest_of_the_url)
        protocol = uri_parts.scheme if host_with_protocol else None
        host = self._parse_netloc_to_hostname(uri_parts)
        self.host = self._create_host(protocol, host)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = unquote(uri_parts.username) if uri_parts.username else uri_parts.username
        self.password = unquote(uri_parts.password) if uri_parts.password else uri_parts.password
        self.port = uri_parts.port
        if uri_parts.query:
            query = dict(parse_qsl(uri_parts.query, keep_blank_values=True))
            if self.EXTRA_KEY in query:
                self.extra = query[self.EXTRA_KEY]
            else:
                self.extra = json.dumps(query)

    @classmethod
    def from_json(cls, value: str, conn_id: str | None = None) -> Connection:
        """
        Create a Connection object from a JSON string.

        :param value: The JSON string containing connection parameters
        :param conn_id: Optional connection ID
        :return: A Connection instance
        """
        kwargs = json.loads(value)
        extra = kwargs.pop("extra", None)
        if extra:
            kwargs["extra"] = extra if isinstance(extra, str) else json.dumps(extra)
        conn_type = kwargs.pop("conn_type", None)
        if conn_type:
            kwargs["conn_type"] = cls._normalize_conn_type(conn_type) or ""
        port = kwargs.pop("port", None)
        if port:
            try:
                kwargs["port"] = int(port)
            except ValueError:
                raise ValueError(f"Expected integer value for `port`, but got {port!r} instead.")
        # Ensure conn_id is not None before passing to Connection constructor
        return Connection(conn_id=conn_id or "unknown", **kwargs)

    def as_json(self) -> str:
        """
        Convert Connection to a JSON string.

        :return: A JSON representation of the connection
        """
        conn_dict = attrs.asdict(self)
        # Remove conn_id as it's not part of the connection parameters
        conn_dict.pop("conn_id", None)
        # Remove None values for cleaner output
        conn_dict = {k: v for k, v in conn_dict.items() if v is not None}
        return json.dumps(conn_dict)
