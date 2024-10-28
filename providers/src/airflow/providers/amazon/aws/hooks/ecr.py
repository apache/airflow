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

import base64
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.log.secrets_masker import mask_secret

if TYPE_CHECKING:
    from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EcrCredentials:
    """Helper (frozen dataclass) for storing temporary ECR credentials."""

    username: str
    password: str
    proxy_endpoint: str
    expires_at: datetime

    def __post_init__(self):
        """Initialize the `Ecr` credentials object."""
        mask_secret(self.password)
        logger.debug(
            "Credentials to Amazon ECR %r expires at %s.",
            self.proxy_endpoint,
            self.expires_at,
        )

    @property
    def registry(self) -> str:
        """Return registry in appropriate `docker login` format."""
        # https://github.com/docker/docker-py/issues/2256#issuecomment-824940506
        return self.proxy_endpoint.replace("https://", "")


class EcrHook(AwsBaseHook):
    """
    Interact with Amazon Elastic Container Registry (ECR).

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("ecr") <ECR.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, **kwargs):
        kwargs["client_type"] = "ecr"
        super().__init__(**kwargs)

    def get_temporary_credentials(
        self, registry_ids: list[str] | str | None = None
    ) -> list[EcrCredentials]:
        """
        Get temporary credentials for Amazon ECR.

        .. seealso::
            - :external+boto3:py:meth:`ECR.Client.get_authorization_token`

        :param registry_ids: Either AWS Account ID or list of AWS Account IDs that are associated
            with the registries from which credentials are obtained. If you do not specify a registry,
            the default registry is assumed.
        :return: list of :class:`airflow.providers.amazon.aws.hooks.ecr.EcrCredentials`,
            obtained credentials valid for 12 hours.
        """
        registry_ids = registry_ids or None
        if isinstance(registry_ids, str):
            registry_ids = [registry_ids]

        if registry_ids:
            response = self.conn.get_authorization_token(registryIds=registry_ids)
        else:
            response = self.conn.get_authorization_token()

        creds = []
        for auth_data in response["authorizationData"]:
            username, password = (
                base64.b64decode(auth_data["authorizationToken"])
                .decode("utf-8")
                .split(":")
            )
            creds.append(
                EcrCredentials(
                    username=username,
                    password=password,
                    proxy_endpoint=auth_data["proxyEndpoint"],
                    expires_at=auth_data["expiresAt"],
                )
            )

        return creds
