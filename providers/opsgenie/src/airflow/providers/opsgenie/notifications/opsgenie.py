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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.opsgenie.hooks.opsgenie import OpsgenieAlertHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.opsgenie.typing.opsgenie import CreateAlertPayload


class OpsgenieNotifier(BaseNotifier):
    """
    This notifier allows you to post alerts to Opsgenie.

    Accepts a connection that has an Opsgenie API key as the connection's password.
    This notifier sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this notifier.

    .. seealso::
        For more information on how to use this notifier, take a look at the guide:
        :ref:`howto/notifier:OpsgenieNotifier`

    :param payload: The payload necessary for creating an alert.
    :param opsgenie_conn_id: Optional. The name of the Opsgenie connection to use. Default conn_id is opsgenie_default
    """

    template_fields: Sequence[str] = ("payload",)

    def __init__(
        self,
        *,
        payload: CreateAlertPayload,
        opsgenie_conn_id: str = "opsgenie_default",
    ) -> None:
        super().__init__()

        self.payload = payload
        self.opsgenie_conn_id = opsgenie_conn_id

    @cached_property
    def hook(self) -> OpsgenieAlertHook:
        """Opsgenie alert Hook."""
        return OpsgenieAlertHook(self.opsgenie_conn_id)

    def notify(self, context: Context) -> None:
        """Call the OpsgenieAlertHook to post message."""
        self.hook.get_conn().create_alert(self.payload)


send_opsgenie_notification = OpsgenieNotifier
