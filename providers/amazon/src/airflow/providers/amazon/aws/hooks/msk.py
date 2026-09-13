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
"""This module contains Amazon Managed Streaming for Apache Kafka hook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from botocore.credentials import CredentialProvider

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

if TYPE_CHECKING:
    from botocore.credentials import Credentials


class _MskCredentialsProvider(CredentialProvider):
    METHOD = "airflow"

    def __init__(self, hook: MskHook) -> None:
        self.hook = hook

    def load(self) -> Credentials | None:
        return self.hook.get_session(region_name=self.hook.region_name).get_credentials()


class MskHook(AwsBaseHook):
    """
    Create an authenticated Amazon Managed Streaming for Apache Kafka client.

    Additional arguments, such as ``aws_conn_id`` or ``region_name``, are passed
    to :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`. Use
    ``conn`` or ``get_conn()`` to access the underlying ``boto3.client("kafka")``
    directly.
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kafka"
        super().__init__(*args, **kwargs)

    def confluent_token(self, config_str: str) -> tuple[str, float]:
        """Generate an Amazon MSK IAM token for a ``confluent_kafka`` OAuth callback."""
        if not self.region_name:
            raise ValueError("AWS region is required to generate an Amazon MSK IAM token")

        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(
            self.region_name, _MskCredentialsProvider(self)
        )
        return token, expiry_ms / 1000
