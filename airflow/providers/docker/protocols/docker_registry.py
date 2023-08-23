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

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from airflow.exceptions import AirflowNotFoundException
from airflow.utils.log.secrets_masker import mask_secret

if TYPE_CHECKING:
    from airflow.models import Connection


@dataclass(frozen=True)
class DockerRegistryCredentials:
    """
    Helper (frozen dataclass) for storing Docker Registry credentials.

    Class instance attributes will pass into the ``docker.APIClient.login`` method.
    """

    username: str
    password: str
    registry: str
    email: str | None = None
    reauth: bool = False

    def __post_init__(self):
        if self.password:
            # Mask password if it specified.
            mask_secret(self.password)


@runtime_checkable
class DockerRegistryAuthProtocol(Protocol):
    """Docker Registry Authentication Protocol."""

    @abstractmethod
    def get_credentials(self, *, conn: Connection | None) -> list[DockerRegistryCredentials]:
        """Obtain Docker registry credentials.

        :param conn: :ref:`Docker connection id <howto/connection:docker>` where stored credentials
            to Docker Registry.
        """
        ...


@runtime_checkable
class RefreshableDockerRegistryAuthProtocol(DockerRegistryAuthProtocol, Protocol):
    """Refreshable by Airflow Docker Registry Authentication Protocol."""

    # need_refresh: bool

    @property
    @abstractmethod
    def need_refresh(self) -> bool:
        """Is required refresh credentials or not."""
        ...

    @abstractmethod
    def refresh_credentials(self, *, conn: Connection | None) -> list[DockerRegistryCredentials]:
        """Refresh Docker registry credentials.

        :param conn: :ref:`Docker connection id <howto/connection:docker>` where stored credentials
            to Docker Registry.
        """
        ...


class NoDockerRegistryAuth(DockerRegistryAuthProtocol):
    """Class provided empty Docker Registry Credentials."""

    def get_credentials(self, *, conn: Connection | None):
        """``NoDockerRegistryAuth`` always return empty list."""
        return []


class AirflowConnectionDockerRegistryAuth(DockerRegistryAuthProtocol):
    """Obtain Docker Registry credentials from Airflow Connection."""

    def get_credentials(self, *, conn: Connection | None):
        """Get credentials from Airflow Connection."""
        if not conn:
            raise AirflowNotFoundException("Docker Connection not set.")
        if not conn.host:
            raise AirflowNotFoundException("No Docker Registry URL provided.")
        if not conn.login:
            raise AirflowNotFoundException("No Docker Registry username provided.")

        conn_extra = conn.extra_dejson

        creds = DockerRegistryCredentials(
            username=conn.login,
            password=conn.password,
            registry=f"{conn.host}:{conn.port}" if conn.port else conn.host,
            email=self.get_email(conn_extra),
            reauth=self.get_reauth(conn_extra),
        )
        return [creds]

    @staticmethod
    def get_reauth(conn_extra: dict) -> bool:
        """Get `reauth` from Airflow Connection."""
        reauth = conn_extra.get("reauth", True)
        if isinstance(reauth, str):
            reauth = reauth.lower()
            if reauth in ("y", "yes", "t", "true", "on", "1"):
                reauth = True
            elif reauth in ("n", "no", "f", "false", "off", "0"):
                reauth = False
            else:
                raise ValueError(f"Unable parse `reauth` value {reauth!r} to bool.")
        return reauth

    @staticmethod
    def get_email(conn_extra: dict) -> str | None:
        """Get `email` for the registry account from Airflow Connection."""
        return conn_extra.get("email") or None
