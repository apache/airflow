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

"""This module contains a web hook for Chime."""

from __future__ import annotations

import json
import re
from functools import cached_property
from typing import Any

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class ChimeWebhookHook(HttpHook):
    """
    Interact with Amazon Chime Webhooks to create notifications.

    .. warning:: This hook is only designed to work with web hooks and not chatbots.

    :param chime_conn_id: :ref:`Amazon Chime Connection ID <howto/connection:chime>`
        with Endpoint as `https://hooks.chime.aws` and the webhook token
        in the form of ``{webhook.id}?token{webhook.token}``
    """

    conn_name_attr = "chime_conn_id"
    default_conn_name = "chime_default"
    conn_type = "chime"
    hook_name = "Amazon Chime Webhook"

    def __init__(
        self,
        chime_conn_id: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._chime_conn_id = chime_conn_id

    @cached_property
    def webhook_endpoint(self):
        return self._get_webhook_endpoint(self._chime_conn_id)

    def _get_webhook_endpoint(self, conn_id: str) -> str:
        """
        Given a Chime conn_id return the default webhook endpoint.

        :param conn_id: The provided connection ID.
        :return: Endpoint(str) for chime webhook.
        """
        conn = self.get_connection(conn_id)
        token = conn.password
        if token is None:
            raise AirflowException("Webhook token field is missing and is required.")
        if not conn.schema:
            raise AirflowException("Webook schema field is missing and is required")
        if not conn.host:
            raise AirflowException("Webhook host field is missing and is required.")
        url = conn.schema + "://" + conn.host
        endpoint = url + token
        # Check to make sure the endpoint matches what Chime expects
        if not re.fullmatch(r"[a-zA-Z0-9_-]+\?token=[a-zA-Z0-9_-]+", token):
            raise AirflowException(
                "Expected Chime webhook token in the form of '{webhook.id}?token={webhook.token}'."
            )

        return endpoint

    def _build_chime_payload(self, message: str) -> str:
        """
        Build payload for Chime and ensures messages do not exceed max length allowed.

        :param message: The message you want to send to your Chime room. (max 4096 characters)
        """
        payload: dict[str, Any] = {}
        # We need to make sure that the message does not exceed the max length for Chime
        if len(message) > 4096:
            raise AirflowException("Chime message must be 4096 characters or less.")

        payload["Content"] = message
        return json.dumps(payload)

    def send_message(self, message: str) -> None:
        """
        Execute calling the Chime webhook endpoint.

        :param message: The message you want to send to your Chime room.(max 4096 characters)
        """
        chime_payload = self._build_chime_payload(message)
        self.run(
            endpoint=self.webhook_endpoint, data=chime_payload, headers={"Content-type": "application/json"}
        )

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour to only get what is needed for Chime webhooks to function."""
        return {
            "hidden_fields": ["login", "port", "extra"],
            "relabeling": {
                "host": "Chime Webhook Endpoint",
                "password": "Chime Webhook token",
            },
            "placeholders": {
                "schema": "https",
                "host": "hooks.chime.aws/incomingwebhook/",
                "password": "T00000000?token=XXXXXXXXXXXXXXXXXXXXXXXX",
            },
        }
