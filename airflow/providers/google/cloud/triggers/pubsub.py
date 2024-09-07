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
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Sequence

from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.providers.google.cloud.hooks.pubsub import PubSubAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    :param messages_callback: (Optional) Callback to process received messages.
        Its return value will be saved to XCom.
        If you are pulling large messages, you probably want to provide a custom callback.
        If not provided, the default implementation will convert `ReceivedMessage` objects
        into JSON-serializable dicts using `google.protobuf.json_format.MessageToDict` function.
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
        messages_callback: Callable[[list[ReceivedMessage], Context], Any] | None = None,
        poke_interval: float = 10.0,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.ack_messages = ack_messages
        self.messages_callback = messages_callback
        self.poke_interval = poke_interval
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.hook = PubSubAsyncHook()

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize PubsubPullTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.pubsub.PubsubPullTrigger",
            {
                "project_id": self.project_id,
                "subscription": self.subscription,
                "max_messages": self.max_messages,
                "ack_messages": self.ack_messages,
                "messages_callback": self.messages_callback,
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
