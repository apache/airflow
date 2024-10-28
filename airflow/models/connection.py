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
from contextlib import suppress
from json import JSONDecodeError
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

import re2
from sqlalchemy import Boolean, Column, Integer, String, Text
from sqlalchemy.orm import declared_attr, reconstructor, synonym

from airflow.configuration import ensure_secrets_loaded
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models.base import ID_LEN, Base
from airflow.models.crypto import get_fernet
from airflow.secrets.cache import SecretCache
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)
# sanitize the `conn_id` pattern by allowing alphanumeric characters plus
# the symbols #,!,-,_,.,:,\,/ and () requiring at least one match.
#
# You can try the regex here: https://regex101.com/r/69033B/1
RE_SANITIZE_CONN_ID = re2.compile(r"^[\w\#\!\(\)\-\.\:\/\\]{1,}$")
# the conn ID max len should be 250
CONN_ID_MAX_LEN: int = 250


def sanitize_conn_id(conn_id: str | None, max_length=CONN_ID_MAX_LEN) -> str | None:
    r"""
    Sanitizes the connection id and allows only specific characters to be within.

    Namely, it allows alphanumeric characters plus the symbols #,!,-,_,.,:,\,/ and () from 1 and up to
    250 consecutive matches. If desired, the max length can be adjusted by setting `max_length`.

    You can try to play with the regex here: https://regex101.com/r/69033B/1

    The character selection is such that it prevents the injection of javascript or
    executable bits to avoid any awkward behaviour in the front-end.

    :param conn_id: The connection id to sanitize.
    :param max_length: The max length of the connection ID, by default it is 250.
    :return: the sanitized string, `None` otherwise.
    """
    # check if `conn_id` or our match group is `None` and the `conn_id` is within the specified length.
    if (not isinstance(conn_id, str) or len(conn_id) > max_length) or (
        res := re2.match(RE_SANITIZE_CONN_ID, conn_id)
    ) is None:
        return None

    # if we reach here, then we matched something, return the first match
    return res.group(0)


def _parse_netloc_to_hostname(uri_parts):
    """
    Parse a URI string to get the correct Hostname.

    ``urlparse(...).hostname`` or ``urlsplit(...).hostname`` returns value into the lowercase in most cases,
    there are some exclusion exists for specific cases such as https://bugs.python.org/issue32323
    In case if expected to get a path as part of hostname path,
    then default behavior ``urlparse``/``urlsplit`` is unexpected.
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


class Connection(Base, LoggingMixin):
    """
    Placeholder to store information about different database instances connection information.

    The idea here is that scripts use references to database instances (conn_id)
    instead of hard coding hostname, logins and passwords when using operators or hooks.

    .. seealso::
        For more information on how to use this class, see: :doc:`/howto/connection`

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
    :param uri: URI address describing connection parameters.
    """

    EXTRA_KEY = "__extra__"

    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN), unique=True, nullable=False)
    conn_type = Column(String(500), nullable=False)
    description = Column(
        Text().with_variant(Text(5000), "mysql").with_variant(String(5000), "sqlite")
    )
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(Text())
    _password = Column("password", Text())
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    _extra = Column("extra", Text())

    def __init__(
        self,
        conn_id: str | None = None,
        conn_type: str | None = None,
        description: str | None = None,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | dict | None = None,
        uri: str | None = None,
    ):
        super().__init__()
        self.conn_id = sanitize_conn_id(conn_id)
        self.description = description
        if extra and not isinstance(extra, str):
            extra = json.dumps(extra)
        if uri and (conn_type or host or login or password or schema or port or extra):
            raise AirflowException(
                "You must create an object using the URI or individual values "
                "(conn_type, host, login, password, schema, port or extra)."
                "You can't mix these two ways to create this object."
            )
        if uri:
            self._parse_from_uri(uri)
        else:
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra
        if self.extra:
            self._validate_extra(self.extra, self.conn_id)

        if self.password:
            mask_secret(self.password)
            mask_secret(quote(self.password))

    @staticmethod
    def _validate_extra(extra, conn_id) -> None:
        """Verify that ``extra`` is a JSON-encoded Python dict."""
        if extra is None:
            return None
        try:
            extra_parsed = json.loads(extra)
            if not isinstance(extra_parsed, dict):
                raise ValueError(
                    "Encountered JSON value in `extra` which does not parse as a dictionary in "
                    f"connection {conn_id!r}. The `extra` field must contain a JSON "
                    "representation of a Python dict."
                )
        except json.JSONDecodeError:
            raise ValueError(
                f"Encountered non-JSON in `extra` field for connection {conn_id!r}."
            )
        return None

    @reconstructor
    def on_db_load(self):
        if self.password:
            mask_secret(self.password)
            mask_secret(quote(self.password))

    @staticmethod
    def _normalize_conn_type(conn_type):
        if conn_type == "postgresql":
            conn_type = "postgres"
        elif "-" in conn_type:
            conn_type = conn_type.replace("-", "_")
        return conn_type

    def _parse_from_uri(self, uri: str):
        schemes_count_in_uri = uri.count("://")
        if schemes_count_in_uri > 2:
            raise AirflowException(f"Invalid connection string: {uri}.")
        host_with_protocol = schemes_count_in_uri == 2
        uri_parts = urlsplit(uri)
        conn_type = uri_parts.scheme
        self.conn_type = self._normalize_conn_type(conn_type)
        rest_of_the_url = uri.replace(
            f"{conn_type}://", ("" if host_with_protocol else "//")
        )
        if host_with_protocol:
            uri_splits = rest_of_the_url.split("://", 1)
            if "@" in uri_splits[0] or ":" in uri_splits[0]:
                raise AirflowException(f"Invalid connection string: {uri}.")
        uri_parts = urlsplit(rest_of_the_url)
        protocol = uri_parts.scheme if host_with_protocol else None
        host = _parse_netloc_to_hostname(uri_parts)
        self.host = self._create_host(protocol, host)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = (
            unquote(uri_parts.username) if uri_parts.username else uri_parts.username
        )
        self.password = (
            unquote(uri_parts.password) if uri_parts.password else uri_parts.password
        )
        self.port = uri_parts.port
        if uri_parts.query:
            query = dict(parse_qsl(uri_parts.query, keep_blank_values=True))
            if self.EXTRA_KEY in query:
                self.extra = query[self.EXTRA_KEY]
            else:
                self.extra = json.dumps(query)

    @staticmethod
    def _create_host(protocol, host) -> str | None:
        """Return the connection host with the protocol."""
        if not host:
            return host
        if protocol:
            return f"{protocol}://{host}"
        return host

    def get_uri(self) -> str:
        """Return connection in URI format."""
        if self.conn_type and "_" in self.conn_type:
            self.log.warning(
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
            protocol, host = None, self.host

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
        if host:
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
            if query and self.extra_dejson == dict(
                parse_qsl(query, keep_blank_values=True)
            ):
                uri += ("?" if self.schema else "/?") + query
            else:
                uri += ("?" if self.schema else "/?") + urlencode(
                    {self.EXTRA_KEY: self.extra}
                )

        return uri

    def get_password(self) -> str | None:
        """Return encrypted password."""
        if self._password and self.is_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    f"Can't decrypt encrypted password for login={self.login}  "
                    f"FERNET_KEY configuration is missing"
                )
            return fernet.decrypt(bytes(self._password, "utf-8")).decode()
        else:
            return self._password

    def set_password(self, value: str | None):
        """Encrypt password and set in object attribute."""
        if value:
            fernet = get_fernet()
            self._password = fernet.encrypt(bytes(value, "utf-8")).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def password(cls):
        """Password. The value is decrypted/encrypted when reading/setting the value."""
        return synonym(
            "_password", descriptor=property(cls.get_password, cls.set_password)
        )

    def get_extra(self) -> str:
        """Return encrypted extra-data."""
        if self._extra and self.is_extra_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    f"Can't decrypt `extra` params for login={self.login}, "
                    f"FERNET_KEY configuration is missing"
                )
            extra_val = fernet.decrypt(bytes(self._extra, "utf-8")).decode()
        else:
            extra_val = self._extra
        if extra_val:
            self._validate_extra(extra_val, self.conn_id)
        return extra_val

    def set_extra(self, value: str):
        """Encrypt extra-data and save in object attribute to object."""
        if value:
            self._validate_extra(value, self.conn_id)
            fernet = get_fernet()
            self._extra = fernet.encrypt(bytes(value, "utf-8")).decode()
            self.is_extra_encrypted = fernet.is_encrypted
        else:
            self._extra = value
            self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):
        """Extra data. The value is decrypted/encrypted when reading/setting the value."""
        return synonym("_extra", descriptor=property(cls.get_extra, cls.set_extra))

    def rotate_fernet_key(self):
        """Encrypts data with a new key. See: :ref:`security/fernet`."""
        fernet = get_fernet()
        if self._password and self.is_encrypted:
            self._password = fernet.rotate(self._password.encode("utf-8")).decode()
        if self._extra and self.is_extra_encrypted:
            self._extra = fernet.rotate(self._extra.encode("utf-8")).decode()

    def get_hook(self, *, hook_params=None):
        """Return hook based on conn_type."""
        from airflow.providers_manager import ProvidersManager

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
        return hook_class(
            **{hook.connection_id_attribute_name: self.conn_id}, **hook_params
        )

    def __repr__(self):
        return self.conn_id or ""

    def test_connection(self):
        """Calls out get_hook method and executes test_connection method on that."""
        status, message = False, ""
        try:
            hook = self.get_hook()
            if getattr(hook, "test_connection", False):
                status, message = hook.test_connection()
            else:
                message = f"Hook {hook.__class__.__name__} doesn't implement or inherit test_connection method"
        except Exception as e:
            message = str(e)

        return status, message

    def get_extra_dejson(self, nested: bool = False) -> dict:
        """
        Deserialize extra property to JSON.

        :param nested: Determines whether nested structures are also deserialized into JSON (default False).
        """
        extra = {}

        if self.extra:
            try:
                if nested:
                    for key, value in json.loads(self.extra).items():
                        extra[key] = value
                        if isinstance(value, str):
                            with suppress(JSONDecodeError):
                                extra[key] = json.loads(value)
                else:
                    extra = json.loads(self.extra)
            except JSONDecodeError:
                self.log.exception("Failed parsing the json for conn_id %s", self.conn_id)

            # Mask sensitive keys from this list
            mask_secret(extra)

        return extra

    @property
    def extra_dejson(self) -> dict:
        """Returns the extra property by deserializing json."""
        return self.get_extra_dejson()

    @classmethod
    def get_connection_from_secrets(cls, conn_id: str) -> Connection:
        """
        Get connection by conn_id.

        :param conn_id: connection id
        :return: connection
        """
        # check cache first
        # enabled only if SecretCache.init() has been called first
        try:
            uri = SecretCache.get_connection_uri(conn_id)
            return Connection(conn_id=conn_id, uri=uri)
        except SecretCache.NotPresentException:
            pass  # continue business

        # iterate over backends if not in cache (or expired)
        for secrets_backend in ensure_secrets_loaded():
            try:
                conn = secrets_backend.get_connection(conn_id=conn_id)
                if conn:
                    SecretCache.save_connection_uri(conn_id, conn.get_uri())
                    return conn
            except Exception:
                log.exception(
                    "Unable to retrieve connection from secrets backend (%s). "
                    "Checking subsequent secrets backend.",
                    type(secrets_backend).__name__,
                )

        raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")

    def to_dict(
        self, *, prune_empty: bool = False, validate: bool = True
    ) -> dict[str, Any]:
        """
        Convert Connection to json-serializable dictionary.

        :param prune_empty: Whether or not remove empty values.
        :param validate: Validate dictionary is JSON-serializable

        :meta private:
        """
        conn = {
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
            conn = prune_dict(val=conn, mode="strict")
        if (extra := self.extra_dejson) or not prune_empty:
            conn["extra"] = extra

        if validate:
            json.dumps(conn)
        return conn

    @classmethod
    def from_json(cls, value, conn_id=None) -> Connection:
        kwargs = json.loads(value)
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
                raise ValueError(
                    f"Expected integer value for `port`, but got {port!r} instead."
                )
        return Connection(conn_id=conn_id, **kwargs)

    def as_json(self) -> str:
        """Convert Connection to JSON-string object."""
        conn_repr = self.to_dict(prune_empty=True, validate=False)
        conn_repr.pop("conn_id", None)
        return json.dumps(conn_repr)
