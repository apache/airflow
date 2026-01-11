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
"""This module contains a Google PubSub sensor."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import ReceivedMessage

from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator, conf
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class PubSubMessageTransformException(AirflowException):
    """Raise when messages failed to convert pubsub received format."""


class PubSubPullSensor(BaseSensorOperator):
    """
    Pulls messages from a PubSub subscription and passes them through XCom.

    Always waits for at least one message to be returned from the subscription.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubPullSensor`

    .. seealso::
        If you don't want to wait for at least one message to come, use Operator instead:
        :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator`

    This sensor operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the poke method's criteria will be fulfilled and the messages will be
    returned from the operator and passed through XCom for downstream tasks.

    If ``ack_messages`` is set to True, messages will be immediately
    acknowledged before being returned, otherwise, downstream tasks will be
    responsible for acknowledging them.

    If you want a non-blocking task that does not to wait for messages, please use
    :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator`
    instead.

    ``project_id`` and ``subscription`` are templated so you can use
    variables in them.

    :param project_id: the Google Cloud project ID for the subscription (templated)
    :param subscription: the Pub/Sub subscription name. Do not include the
        full subscription path.
    :param max_messages: The maximum number of messages to retrieve per
        PubSub pull request
    :param return_immediately: If this field set to true, the system will
        respond immediately even if it there are no messages available to
        return in the ``Pull`` response. Otherwise, the system may wait
        (for a bounded amount of time) until at least one message is available,
        rather than returning no messages. Warning: setting this field to
        ``true`` is discouraged because it adversely impacts the performance
        of ``Pull`` operations. We recommend that users do not set this field.
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param messages_callback: (Optional) Callback to process received messages.
        Its return value will be saved to XCom.
        If you are pulling large messages, you probably want to provide a custom callback.
        If not provided, the default implementation will convert `ReceivedMessage` objects
        into JSON-serializable dicts using `google.protobuf.json_format.MessageToDict` function.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields: Sequence[str] = (
        "project_id",
        "subscription",
        "impersonation_chain",
    )
    ui_color = "#ff7f50"

    def __init__(
        self,
        *,
        project_id: str,
        subscription: str,
        max_messages: int = 5,
        return_immediately: bool = True,
        ack_messages: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        messages_callback: Callable[[list[ReceivedMessage], Context], Any] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        poke_interval: float = 10.0,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.ack_messages = ack_messages
        self.messages_callback = messages_callback
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self._return_value = None

    def poke(self, context: Context) -> bool:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        pulled_messages = hook.pull(
            project_id=self.project_id,
            subscription=self.subscription,
            max_messages=self.max_messages,
            return_immediately=self.return_immediately,
        )

        handle_messages = self.messages_callback or self._default_message_callback

        self._return_value = handle_messages(pulled_messages, context)

        if pulled_messages and self.ack_messages:
            hook.acknowledge(
                project_id=self.project_id,
                subscription=self.subscription,
                messages=pulled_messages,
            )

        return bool(pulled_messages)

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the triggers if deferrable is True."""
        if not self.deferrable:
            super().execute(context)
            return self._return_value
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=PubsubPullTrigger(
                project_id=self.project_id,
                subscription=self.subscription,
                max_messages=self.max_messages,
                ack_messages=self.ack_messages,
                poke_interval=self.poke_interval,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str | list[str]]) -> Any:
        """If messages_callback is provided, execute it; otherwise, return immediately with trigger event message."""
        if event["status"] == "success":
            self.log.info("Sensor pulls messages: %s", event["message"])
            if self.messages_callback:
                received_messages = self._convert_to_received_messages(event["message"])
                _return_value = self.messages_callback(received_messages, context)
                return _return_value

            return event["message"]
        self.log.info("Sensor failed: %s", event["message"])
        raise AirflowException(event["message"])

    def _convert_to_received_messages(self, messages: Any) -> list[ReceivedMessage]:
        try:
            received_messages = [pubsub_v1.types.ReceivedMessage(msg) for msg in messages]
            return received_messages
        except Exception as e:
            raise PubSubMessageTransformException(
                f"Error converting triggerer event message back to received message format: {e}"
            )

    def _default_message_callback(
        self,
        pulled_messages: list[ReceivedMessage],
        context: Context,
    ):
        """
        Convert `ReceivedMessage` objects into JSON-serializable dicts.

        This method can be overridden by subclasses or by `messages_callback` constructor argument.

        This default implementation converts `ReceivedMessage` objects into JSON-serializable dicts.

        :param pulled_messages: messages received from the topic.
        :param context: same as in `execute`
        :return: value to be saved to XCom.
        """
        messages_json = [ReceivedMessage.to_dict(m) for m in pulled_messages]

        return messages_json
