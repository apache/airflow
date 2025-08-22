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

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType

log = logging.getLogger(__name__)


def _prune_dict(val: Any, mode="strict"):
    """
    Given dict ``val``, returns new dict based on ``val`` with all empty elements removed.

    What constitutes "empty" is controlled by the ``mode`` parameter.  If mode is 'strict'
    then only ``None`` elements will be removed.  If mode is ``truthy``, then element ``x``
    will be removed if ``bool(x) is False``.
    """
    if mode == "strict":
        is_empty = lambda x: x is None
    elif mode == "truthy":
        is_empty = lambda x: not bool(x)
    else:
        raise ValueError("allowable values for `mode` include 'truthy' and 'strict'")

    if isinstance(val, dict):
        new_dict = {}
        for k, v in val.items():
            if is_empty(v):
                continue
            if isinstance(v, (list, dict)):
                new_val = _prune_dict(v, mode=mode)
                if not is_empty(new_val):
                    new_dict[k] = new_val
            else:
                new_dict[k] = v
        return new_dict
    if isinstance(val, list):
        new_list = []
        for v in val:
            if is_empty(v):
                continue
            if isinstance(v, (list, dict)):
                new_val = _prune_dict(v, mode=mode)
                if not is_empty(new_val):
                    new_list.append(new_val)
            else:
                new_list.append(v)
        return new_list
    return val


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
        host_to_use: str | None
        if self.host and "://" in self.host:
            protocol, host = self.host.split("://", 1)
            # If the protocol in host matches the connection type, don't add it again
            if protocol == self.conn_type:
                host_to_use = self.host
                protocol_to_add = None
            else:
                # Different protocol, add it to the URI
                host_to_use = host
                protocol_to_add = protocol
        else:
            host_to_use = self.host
            protocol_to_add = None

        if protocol_to_add:
            uri += f"{protocol_to_add}://"

        authority_block = ""
        if self.login is not None:
            authority_block += quote(self.login, safe="")
        if self.password is not None:
            authority_block += ":" + quote(self.password, safe="")
        if authority_block > "":
            authority_block += "@"
            uri += authority_block

        host_block = ""
        if host_to_use:
            host_block += quote(host_to_use, safe="")
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
        from airflow.sdk.module_loading import import_string

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

        try:
            return _get_connection(conn_id)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.CONNECTION_NOT_FOUND:
                raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined") from None
            raise

    @property
    def extra_dejson(self) -> dict:
        """Returns the extra property by deserializing json."""
        from airflow.sdk.execution_time.secrets_masker import mask_secret

        extra = {}
        if self.extra:
            try:
                extra = json.loads(self.extra)
            except JSONDecodeError:
                log.exception("Failed to deserialize extra property `extra`, returning empty dictionary")
            else:
                mask_secret(extra)

        return extra

    def get_extra_dejson(self) -> dict:
        """Deserialize extra property to JSON."""
        import warnings

        warnings.warn(
            "`get_extra_dejson` is deprecated and will be removed in a future release. ",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.extra_dejson

    def to_dict(self, *, prune_empty: bool = False, validate: bool = True) -> dict[str, Any]:
        """
        Convert Connection to json-serializable dictionary.

        :param prune_empty: Whether or not remove empty values.
        :param validate: Validate dictionary is JSON-serializable

        :meta private:
        """
        conn: dict[str, Any] = {
            "conn_id": self.conn_id,
            "conn_type": self.conn_type,
            "description": self.description,
            "host": self.host,
            "login": self.login,
            "password": self.password,
            "schema": self.schema,
            "port": self.port,
        }
        if prune_empty:
            conn = _prune_dict(val=conn, mode="strict")
        if (extra := self.extra_dejson) or not prune_empty:
            conn["extra"] = extra

        if validate:
            json.dumps(conn)
        return conn

    @classmethod
    def from_json(cls, value, conn_id=None) -> Connection:
        kwargs = json.loads(value)
        conn_type = kwargs.get("conn_type", None)
        if not conn_type:
            raise ValueError(
                "Connection type (conn_type) is required but missing from connection configuration. "
                "Please add 'conn_type' field to your connection definition."
            )
        extra = kwargs.pop("extra", None)
        if extra:
            kwargs["extra"] = extra if isinstance(extra, str) else json.dumps(extra)
        conn_type = kwargs.pop("conn_type", None)
        if conn_type:
            kwargs["conn_type"] = cls._normalize_conn_type(conn_type)
        port = kwargs.pop("port", None)
        if port:
            try:
                kwargs["port"] = int(port)
            except ValueError:
                raise ValueError(f"Expected integer value for `port`, but got {port!r} instead.")
        return cls(conn_id=conn_id, **kwargs)

    def as_json(self) -> str:
        """Convert Connection to JSON-string object."""
        conn_repr = self.to_dict(prune_empty=True, validate=False)
        conn_repr.pop("conn_id", None)
        return json.dumps(conn_repr)

    @staticmethod
    def _normalize_conn_type(conn_type):
        if conn_type == "postgresql":
            conn_type = "postgres"
        elif "-" in conn_type:
            conn_type = conn_type.replace("-", "_")
        return conn_type
