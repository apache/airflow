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

from datetime import datetime, timedelta, timezone
from functools import cached_property

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.ecr import EcrHook

try:
    from airflow.providers.docker.protocols.docker_registry import (
        DockerRegistryCredentials,
        RefreshableDockerRegistryAuthProtocol,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import `airflow.providers.docker.protocols.docker_registry`, "
        "required version of Docker Provider not installed, run: "
        "pip install 'apache-airflow-providers-amazon[docker]'"
    )


class EcrDockerRegistryAuthProtocol(RefreshableDockerRegistryAuthProtocol):
    """Implementation of DockerRegistryAuthProtocol for ECR."""

    def __init__(
        self,
        *,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        registry_ids: list[str] | str | None = None,
    ):
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.registry_ids = registry_ids
        self._expires_at: datetime | None = None

    @property
    def need_refresh(self) -> bool:
        if self.expires_at:
            return (self.expires_at - datetime.now(tz=timezone.utc)) < timedelta(minutes=5)
        return False

    @property
    def expires_at(self) -> datetime | None:
        return self._expires_at

    @expires_at.setter
    def expires_at(self, value: datetime):
        if not self._expires_at or self.need_refresh:
            self._expires_at = value
        elif self._expires_at > value:
            self._expires_at = value  # Use lower value

    @cached_property
    def hook(self) -> EcrHook:
        return EcrHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def get_credentials(self, *, conn: Connection | None) -> list[DockerRegistryCredentials]:
        credentials = []
        registry_ids = self.registry_ids or self.hook.service_config.get("registry_ids", None)
        for ecr_creds in self.hook.get_temporary_credentials(registry_ids=registry_ids):
            self.expires_at = ecr_creds.expires_at
            credentials.append(
                DockerRegistryCredentials(
                    username=ecr_creds.username,
                    password=ecr_creds.password,
                    registry=ecr_creds.registry,
                    reauth=True,
                )
            )
        return credentials

    def refresh_credentials(self, *, conn: Connection | None) -> list[DockerRegistryCredentials]:
        return self.get_credentials(conn=conn)
