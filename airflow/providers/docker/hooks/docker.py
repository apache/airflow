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

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.docker.protocols.docker_registry import (
    AirflowConnectionDockerRegistryAuth,
    DockerRegistryAuthProtocol,
    DockerRegistryCredentials,
    NoDockerRegistryAuth,
    RefreshableDockerRegistryAuthProtocol,
)

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
    :param registry_auth: Object which use for auth to Docker Registry, should implement
        class:`airflow.providers.docker.protocols.docker_registry.DockerRegistryAuthProtocol`,
        if set to ``None`` then auto-assign object depend on value of ``docker_conn_id``.
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
        registry_auth: DockerRegistryAuthProtocol | None = None,
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

        if registry_auth is None:
            # Set registry_auth based on `docker_conn_id` value
            if self.docker_conn_id:
                registry_auth = AirflowConnectionDockerRegistryAuth()
            else:
                registry_auth = NoDockerRegistryAuth()
        elif not isinstance(registry_auth, DockerRegistryAuthProtocol):
            raise TypeError(
                "'registry_auth' expected DockerRegistryAuthProtocol, "
                f"but got {type(registry_auth).__name__}."
            )

        self._registry_auth: DockerRegistryAuthProtocol = registry_auth
        self._api_client: APIClient | None = None

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
    def _airflow_connection(self) -> Connection | None:
        """Return Airflow connection associated with `docker_conn_id` (cached)."""
        if not self.docker_conn_id:
            return None
        return self.get_connection(self.docker_conn_id)

    @property
    def api_client(self) -> APIClient:
        """Create connection to docker host and return ``docker.APIClient``."""
        if not self._api_client:
            # Create client only once
            self._api_client = APIClient(
                base_url=self.__base_url, version=self.__version, tls=self.__tls, timeout=self.__timeout
            )
            self._login(
                self._api_client,
                self._registry_auth.get_credentials(conn=self._airflow_connection),
            )
            self._client_created = True
        elif (
            isinstance(self._registry_auth, RefreshableDockerRegistryAuthProtocol)
            and self._registry_auth.need_refresh
        ):
            self._login(
                self._api_client,
                self._registry_auth.refresh_credentials(conn=self._airflow_connection),
                reauth=True,
            )

        return self._api_client

    def _login(self, client: APIClient, credentials: list[DockerRegistryCredentials], reauth=False) -> None:
        for rc in credentials:
            try:
                self.log.info("Login into Docker Registry: %s", rc.registry)
                client.login(
                    username=rc.username,
                    password=rc.password,
                    registry=rc.registry,
                    email=rc.email,
                    reauth=reauth or rc.reauth,  # Force reauth on refresh credentials
                )
            except APIError:
                self.log.error("Login failed to registry: %s", rc.registry)
                raise
            self.log.debug("Login successful to registry: %s", rc.registry)

    @property
    def client_created(self) -> bool:
        """Is api_client created or not."""
        return self._client_created

    def get_conn(self) -> APIClient:
        """Create connection to docker host and return ``docker.APIClient`` (cached)."""
        return self.api_client

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
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
