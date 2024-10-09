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
"""This module contains Google Cloud Pubsub triggers."""

from __future__ import annotations

import asyncio
from functools import cached_property
from typing import Any, AsyncIterator, Sequence

from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.providers.google.cloud.hooks.pubsub import PubSubAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class PubsubPullTrigger(BaseTrigger):
    """
    Initialize the Pubsub Pull Trigger with needed parameters.

    :param project_id: the Google Cloud project ID for the subscription (templated)
    :param subscription: the Pub/Sub subscription name. Do not include the full subscription path.
    :param max_messages: The maximum number of messages to retrieve per
        PubSub pull request
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :param gcp_conn_id: Reference to google cloud connection id
    :param poke_interval: polling period in seconds to check for the status
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        project_id: str,
        subscription: str,
        max_messages: int,
        ack_messages: bool,
        gcp_conn_id: str,
        poke_interval: float = 10.0,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.ack_messages = ack_messages
        self.poke_interval = poke_interval
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize PubsubPullTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger",
            {
                "project_id": self.project_id,
                "subscription": self.subscription,
                "max_messages": self.max_messages,
                "ack_messages": self.ack_messages,
                "poke_interval": self.poke_interval,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        try:
            while True:
                if pulled_messages := await self.hook.pull(
                    project_id=self.project_id,
                    subscription=self.subscription,
                    max_messages=self.max_messages,
                    return_immediately=True,
                ):
                    if self.ack_messages:
                        await self.message_acknowledgement(pulled_messages)

                    messages_json = [ReceivedMessage.to_dict(m) for m in pulled_messages]

                    yield TriggerEvent({"status": "success", "message": messages_json})
                    return
                self.log.info("Sleeping for %s seconds.", self.poke_interval)
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def message_acknowledgement(self, pulled_messages):
        await self.hook.acknowledge(
            project_id=self.project_id,
            subscription=self.subscription,
            messages=pulled_messages,
        )
        self.log.info("Acknowledged ack_ids from subscription %s", self.subscription)

    @cached_property
    def hook(self) -> PubSubAsyncHook:
        return PubSubAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            project_id=self.project_id,
        )
