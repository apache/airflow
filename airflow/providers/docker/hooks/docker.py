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
from functools import cached_property
from typing import TYPE_CHECKING, Any

from docker import APIClient, TLSConfig
from docker.constants import DEFAULT_TIMEOUT_SECONDS
from docker.errors import APIError

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection


class DockerHook(BaseHook):
    """
    Interact with a Docker Daemon and Container Registry.

    This class provide a thin wrapper around the ``docker.APIClient``.

    .. seealso::
        - :ref:`Docker Connection <howto/connection:docker>`
        - `Docker SDK: Low-level API <https://docker-py.readthedocs.io/en/stable/api.html?low-level-api>`_

    :param docker_conn_id: :ref:`Docker connection id <howto/connection:docker>` where stored credentials
         to Docker Registry. If set to ``None`` or empty then hook does not login to Container Registry.
    :param base_url: URL to the Docker server.
    :param version: The version of the API to use. Use ``auto`` or ``None`` for automatically detect
        the server's version.
    :param tls: Is connection required TLS, for enable pass ``True`` for use with default options,
        or pass a `docker.tls.TLSConfig` object to use custom configurations.
    :param timeout: Default timeout for API calls, in seconds.
    """

    conn_name_attr = "docker_conn_id"
    default_conn_name = "docker_default"
    conn_type = "docker"
    hook_name = "Docker"

    def __init__(
        self,
        docker_conn_id: str | None = default_conn_name,
        base_url: str | None = None,
        version: str | None = None,
        tls: TLSConfig | bool | None = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        if not base_url:
            raise AirflowException("URL to the Docker server not provided.")
        elif tls:
            if base_url.startswith("tcp://"):
                base_url = base_url.replace("tcp://", "https://")
                self.log.debug("Change `base_url` schema from 'tcp://' to 'https://'.")
            if not base_url.startswith("https://"):
                self.log.warning("When `tls` specified then `base_url` expected 'https://' schema.")

        self.docker_conn_id = docker_conn_id
        self.__base_url = base_url
        self.__version = version
        self.__tls = tls or False
        self.__timeout = timeout
        self._client_created = False

    @staticmethod
    def construct_tls_config(
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        verify: bool = True,
        assert_hostname: str | bool | None = None,
        ssl_version: str | None = None,
    ) -> TLSConfig | bool:
        """
        Construct TLSConfig object from parts.

        :param ca_cert: Path to a PEM-encoded CA (Certificate Authority) certificate file.
        :param client_cert: Path to PEM-encoded certificate file.
        :param client_key: Path to PEM-encoded key file.
        :param verify: Set ``True`` to verify the validity of the provided certificate.
        :param assert_hostname: Hostname to match against the docker server certificate
            or ``False`` to disable the check.
        :param ssl_version: Version of SSL to use when communicating with docker daemon.
        """
        if ca_cert and client_cert and client_key:
            # Ignore type error on SSL version here.
            # It is deprecated and type annotation is wrong, and it should be string.
            return TLSConfig(
                ca_cert=ca_cert,
                client_cert=(client_cert, client_key),
                verify=verify,
                ssl_version=ssl_version,
                assert_hostname=assert_hostname,
            )
        return False

    @cached_property
    def api_client(self) -> APIClient:
        """Create connection to docker host and return ``docker.APIClient`` (cached)."""
        client = APIClient(
            base_url=self.__base_url, version=self.__version, tls=self.__tls, timeout=self.__timeout
        )
        if self.docker_conn_id:
            # Obtain connection and try to login to Container Registry only if ``docker_conn_id`` set.
            self.__login(client, self.get_connection(self.docker_conn_id))

        self._client_created = True
        return client

    @property
    def client_created(self) -> bool:
        """Is api_client created or not."""
        return self._client_created

    def get_conn(self) -> APIClient:
        """Create connection to docker host and return ``docker.APIClient`` (cached)."""
        return self.api_client

    def __login(self, client, conn: Connection) -> None:
        if not conn.host:
            raise AirflowNotFoundException("No Docker Registry URL provided.")
        if not conn.login:
            raise AirflowNotFoundException("No Docker Registry username provided.")

        registry = f"{conn.host}:{conn.port}" if conn.port else conn.host

        # Parse additional optional parameters
        email = conn.extra_dejson.get("email") or None
        reauth = conn.extra_dejson.get("reauth", True)
        if isinstance(reauth, str):
            reauth = reauth.lower()
            if reauth in ("y", "yes", "t", "true", "on", "1"):
                reauth = True
            elif reauth in ("n", "no", "f", "false", "off", "0"):
                reauth = False
            else:
                raise ValueError(f"Unable parse `reauth` value {reauth!r} to bool.")

        try:
            self.log.info("Login into Docker Registry: %s", registry)
            client.login(
                username=conn.login, password=conn.password, registry=registry, email=email, reauth=reauth
            )
            self.log.debug("Login successful")
        except APIError:
            self.log.error("Login failed")
            raise

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection form widgets."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "reauth": BooleanField(
                lazy_gettext("Reauthenticate"),
                description="Whether or not to refresh existing authentication on the Docker server.",
            ),
            "email": StringField(lazy_gettext("Email"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "host": "Registry URL",
                "login": "Username",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "reauth": False,
                        "email": "Jane.Doe@example.org",
                    }
                )
            },
        }
