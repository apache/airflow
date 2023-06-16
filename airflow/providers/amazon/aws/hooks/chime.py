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
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class ChimeWebhookHook(HttpHook):
    """Interact with Chime Web Hooks to create notifications.

    .. warning:: This hook is only designed to work with web hooks and not chat bots.

    :param http_conn_id: Http connection ID with host as "https://hooks.chime.aws" and
                         default webhook endpoint in the extra field in the form of
                         ``{"webhook_endpoint": "incomingwebhooks/{webhook.id}?token{webhook.token}"}``
    :param webhook_endpoint: Chime webhook endpoint in the form of
                             ``"incomingwebhooks/{webhook.id}?token={webhook.token}"``
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "chime_default"
    conn_type = "chime"
    hook_name = "Chime Web Hook"

    def __init__(
        self,
        http_conn_id: str | None = None,
        webhook_endpoint: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.webhook_endpoint = self._get_webhook_endpoint(http_conn_id, webhook_endpoint)

    def _get_webhook_endpoint(self, http_conn_id: str | None, webhook_endpoint: str | None) -> str:
        """
        Given a Chime http_conn_id return the default webhook endpoint or override if
        webhook_endpoint is manually provided.

        :param http_conn_id: The provided connection ID.
        :param webhook_endpoint: The manually provided endpoint for the chime webhook.
        :return: Endpoint(str) for chime webhook.
        """
        if webhook_endpoint:
            endpoint = webhook_endpoint
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            endpoint = extra.get("webhook_endpoint", "")
        else:
            raise AirflowException(
                "Missing one of http_conn_id or webhook_endpoint arguments which are required."
            )

        # Check to make sure the endpoint matches what Chime expects
        if not re.match("^incomingwebhooks/[a-zA-Z0-9_-]+\?token=[a-zA-Z0-9_-]+$", endpoint):
            raise AirflowException(
                "Expected Chime webhook endpoint in the form of "
                '"incomingwebhooks/{webhook.id}?token={webhook.token}".'
            )

        return endpoint

    def _build_chime_payload(self, message: str) -> str:
        """
        Builds payload for Chime and ensures messages do not exceed max length allowed.

        :param message: The message you want to send to your Chime room.
                    (max 4096 characters)
        """
        payload: dict[str, Any] = {}
        # We need to make sure that the message does not exceed the max length for Chime
        if len(message) <= 4096:
            payload["Content"] = message
        else:
            raise AirflowException("Chime message must be 4096 characters or less.")

        return json.dumps(payload)

    def send_message(self, message: str) -> None:
        """Execute calling the Chime webhook endpoint.

        :param message: The message you want to send to your Chime room.
                    (max 4096 characters)

        """
        chime_payload = self._build_chime_payload(message)
        self.run(
            endpoint=self.webhook_endpoint, data=chime_payload, headers={"Content-type": "application/json"}
        )
