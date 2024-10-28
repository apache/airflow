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

from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.chime import ChimeWebhookHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.notifications.basenotifier import BaseNotifier


class ChimeNotifier(BaseNotifier):
    """
    Chime notifier to send messages to a chime room via callbacks.

    :param chime_conn_id: The chime connection to use with Endpoint as "https://hooks.chime.aws" and
        the webhook token in the form of ```{webhook.id}?token{webhook.token}```
    :param message: The message to send to the chime room associated with the webhook.

    """

    template_fields = ("message",)

    def __init__(
        self,
        *,
        chime_conn_id: str,
        message: str = "This is the default chime notifier message",
    ):
        super().__init__()
        self.chime_conn_id = chime_conn_id
        self.message = message

    @cached_property
    def hook(self):
        """To reduce overhead cache the hook for the notifier."""
        return ChimeWebhookHook(chime_conn_id=self.chime_conn_id)

    def notify(self, context: Context) -> None:  # type: ignore[override]
        """Send a message to a Chime Chat Room."""
        self.hook.send_message(message=self.message)


send_chime_notification = ChimeNotifier
