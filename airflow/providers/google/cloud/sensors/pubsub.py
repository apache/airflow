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
import warnings
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from google.cloud.pubsub_v1.types import ReceivedMessage
from google.protobuf.json_format import MessageToDict

from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class PubSubPullSensor(BaseSensorOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.
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

    ``project`` and ``subscription`` are templated so you can use
    variables in them.

    :param project: the Google Cloud project ID for the subscription (templated)
    :type project: str
    :param subscription: the Pub/Sub subscription name. Do not include the
        full subscription path.
    :type subscription: str
    :param max_messages: The maximum number of messages to retrieve per
        PubSub pull request
    :type max_messages: int
    :param return_immediately:
        (Deprecated) This is an underlying PubSub API implementation detail.
        It has no real effect on Sensor behaviour other than some internal wait time before retrying
        on empty queue.
        The Sensor task will (by definition) always wait for a message, regardless of this argument value.

        If you want a non-blocking task that does not to wait for messages, please use
        :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator`
        instead.
    :type return_immediately: bool
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :type ack_messages: bool
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param messages_callback: (Optional) Callback to process received messages.
        It's return value will be saved to XCom.
        If you are pulling large messages, you probably want to provide a custom callback.
        If not provided, the default implementation will convert `ReceivedMessage` objects
        into JSON-serializable dicts using `google.protobuf.json_format.MessageToDict` function.
    :type messages_callback: Optional[Callable[[List[ReceivedMessage], Dict[str, Any]], Any]]
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = [
        'project_id',
        'subscription',
        'impersonation_chain',
    ]
    ui_color = '#ff7f50'

    @apply_defaults
    def __init__(
        self,
        *,
        project_id: str,
        subscription: str,
        max_messages: int = 5,
        return_immediately: bool = True,
        ack_messages: bool = False,
        gcp_conn_id: str = 'google_cloud_default',
        messages_callback: Optional[Callable[[List[ReceivedMessage], Dict[str, Any]], Any]] = None,
        delegate_to: Optional[str] = None,
        project: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass the project_id parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            project_id = project

        if not return_immediately:
            warnings.warn(
                "The return_immediately parameter is deprecated.\n"
                " It exposes what is really just an implementation detail of underlying PubSub API.\n"
                " It has no effect on PubSubPullSensor behaviour.\n"
                " It should be left as default value of True.\n"
                " If is here only because of backwards compatibility.\n"
                " If may be removed in the future.\n",
                DeprecationWarning,
                stacklevel=2,
            )

        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.ack_messages = ack_messages
        self.messages_callback = messages_callback
        self.impersonation_chain = impersonation_chain

        self._return_value = None

    def execute(self, context: dict):
        """Overridden to allow messages to be passed"""
        super().execute(context)
        return self._return_value

    def poke(self, context: dict) -> bool:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
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

    def _default_message_callback(
        self,
        pulled_messages: List[ReceivedMessage],
        context: Dict[str, Any],  # pylint: disable=unused-argument
    ):
        """
        This method can be overridden by subclasses or by `messages_callback` constructor argument.
        This default implementation converts `ReceivedMessage` objects into JSON-serializable dicts.

        :param pulled_messages: messages received from the topic.
        :type pulled_messages: List[ReceivedMessage]
        :param context: same as in `execute`
        :return: value to be saved to XCom.
        """
        messages_json = [MessageToDict(m) for m in pulled_messages]

        return messages_json
