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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.dingding.hooks.dingding import DingdingHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DingdingOperator(BaseOperator):
    """
    This operator allows to send DingTalk message using Custom Robot API.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DingdingOperator`

    :param dingding_conn_id: Dingding connection id that has access token in the password field,
        and optional host name in host field, if host not set than default
        ``https://oapi.dingtalk.com`` will use.
    :param message_type: Message type you want to send to Dingding, support five type so far
        including ``text``, ``link``, ``markdown``, ``actionCard``, ``feedCard``.
    :param message: The message send to chat group
    :param at_mobiles: Remind specific users with this message
    :param at_all: Remind all people in group or not. If True, will overwrite ``at_mobiles``
    """

    template_fields: Sequence[str] = ("message",)
    ui_color = "#4ea4d4"  # Operator icon color

    def __init__(
        self,
        *,
        dingding_conn_id: str = "dingding_default",
        message_type: str = "text",
        message: str | dict | None = None,
        at_mobiles: list[str] | None = None,
        at_all: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dingding_conn_id = dingding_conn_id
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def execute(self, context: Context) -> None:
        self.log.info("Sending Dingding message.")
        hook = DingdingHook(
            self.dingding_conn_id,
            self.message_type,
            self.message,
            self.at_mobiles,
            self.at_all,
        )
        hook.send()
