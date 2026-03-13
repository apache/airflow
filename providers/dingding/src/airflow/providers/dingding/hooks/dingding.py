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
from __future__ import annotations

import json
from typing import Any

import requests
from requests import Session

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class DingdingHook(HttpHook):
    """
    Send message using a DingTalk Custom Robot API.

    .. seealso::
        `How to get webhook token <https://open.dingtalk.com/document/robots/custom-robot-access>`__

    :param dingding_conn_id: Dingding connection id that has access token in the password field,
        and optional host name in host field, if host not set than default
        ``https://oapi.dingtalk.com`` will use.
    :param message_type: Message type you want to send to Dingding, support five type so far
        including ``text``, ``link``, ``markdown``, ``actionCard``, ``feedCard``.
    :param message: The message send to chat group
    :param at_mobiles: Remind specific users with this message
    :param at_all: Remind all people in group or not. If True, will overwrite ``at_mobiles``
    """

    conn_name_attr = "dingding_conn_id"
    default_conn_name = "dingding_default"
    conn_type = "dingding"
    hook_name = "DingTalk Custom Robot (Dingding)"

    def __init__(
        self,
        dingding_conn_id="dingding_default",
        message_type: str = "text",
        message: str | dict | None = None,
        at_mobiles: list[str] | None = None,
        at_all: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(http_conn_id=dingding_conn_id, *args, **kwargs)  # type: ignore[misc]
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def _get_endpoint(self) -> str:
        """Get DingTalk Custom Robot endpoint for sending message."""
        conn = self.get_connection(self.http_conn_id)
        token = conn.password
        if not token:
            raise AirflowException(
                "Dingding token is requests but get nothing, check you conn_id configuration."
            )
        return f"robot/send?access_token={token}"

    def _build_message(self) -> str:
        """Build different type of DingTalk custom robot messages."""
        if self.message_type in ["text", "markdown"]:
            data = {
                "msgtype": self.message_type,
                self.message_type: {"content": self.message} if self.message_type == "text" else self.message,
                "at": {"atMobiles": self.at_mobiles, "isAtAll": self.at_all},
            }
        else:
            data = {"msgtype": self.message_type, self.message_type: self.message}
        return json.dumps(data)

    def get_conn(
        self, headers: dict[Any, Any] | None = None, extra_options: dict[str, Any] | None = None
    ) -> Session:
        """
        Overwrite HttpHook get_conn.

        We just need base_url and headers, and not don't need generic params.

        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: extra options to pass to the connection (ignored)
        """
        conn = self.get_connection(self.http_conn_id)
        self.base_url = conn.host if conn.host else "https://oapi.dingtalk.com"
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def send(self) -> None:
        """Send DingTalk Custom Robot message."""
        support_type = ["text", "link", "markdown", "actionCard", "feedCard"]
        if self.message_type not in support_type:
            raise ValueError(
                f"DingdingWebhookHook only support {support_type} so far, but receive {self.message_type}"
            )

        data = self._build_message()
        self.log.info("Sending Dingding type %s message %s", self.message_type, data)
        resp = self.run(
            endpoint=self._get_endpoint(), data=data, headers={"Content-Type": "application/json"}
        )

        # Success send message will return errcode = 0
        if int(resp.json().get("errcode")) != 0:
            raise AirflowException(f"Send Dingding message failed, receive error message {resp.text}")
        self.log.info("Success Send Dingding message")
