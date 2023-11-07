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

from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowOptionalProviderFeatureException

try:
    from airflow.notifications.basenotifier import BaseNotifier
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import BaseNotifier. This feature is only available in Airflow versions >= 2.6.0"
    )

from airflow.providers.opsgenie.hooks.opsgenie import OpsgenieAlertHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


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

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :param message: The Message of the Opsgenie alert (templated)
    :param alias: Client-defined identifier of the alert (templated)
    :param description: Description field of the alert (templated)
    :param responders: Teams, users, escalations and schedules that
        the alert will be routed to send notifications.
    :param visible_to: Teams and users that the alert will become visible
        to without sending any notification.
    :param actions: Custom actions that will be available for the alert.
    :param tags: Tags of the alert.
    :param details: Map of key-value pairs to use as custom properties of the alert.
    :param entity: Entity field of the alert that is
        generally used to specify which domain alert is related to. (templated)
    :param source: Source field of the alert. Default value is
        IP address of the incoming request.
    :param priority: Priority level of the alert. Default value is P3. (templated)
    :param user: Display name of the request owner.
    :param note: Additional note that will be added while creating the alert. (templated)
    """

    template_fields: Sequence[str] = ("message", "alias", "description", "entity", "priority", "note")

    def __init__(
        self,
        *,
        message: str,
        opsgenie_conn_id: str = "opsgenie_default",
        alias: str | None = None,
        description: str | None = None,
        responders: list[dict] | None = None,
        visible_to: list[dict] | None = None,
        actions: list[str] | None = None,
        tags: list[str] | None = None,
        details: dict | None = None,
        entity: str | None = None,
        source: str | None = None,
        priority: str | None = None,
        user: str | None = None,
        note: str | None = None,
    ) -> None:
        super().__init__()

        self.message = message
        self.opsgenie_conn_id = opsgenie_conn_id
        self.alias = alias
        self.description = description
        self.responders = responders
        self.visible_to = visible_to
        self.actions = actions
        self.tags = tags
        self.details = details
        self.entity = entity
        self.source = source
        self.priority = priority
        self.user = user
        self.note = note
        self.hook: OpsgenieAlertHook | None = None

    def _build_opsgenie_payload(self) -> dict[str, Any]:
        """
        Construct the Opsgenie JSON payload.

        All relevant parameters are combined here to a valid Opsgenie JSON payload.

        :return: Opsgenie payload (dict) to send
        """
        payload = {}

        for key in [
            "message",
            "alias",
            "description",
            "responders",
            "visible_to",
            "actions",
            "tags",
            "details",
            "entity",
            "source",
            "priority",
            "user",
            "note",
        ]:
            val = getattr(self, key)
            if val:
                payload[key] = val
        return payload

    def notify(self, context: Context) -> None:
        """Call the OpsgenieAlertHook to post message."""
        self.hook = OpsgenieAlertHook(self.opsgenie_conn_id)
        self.hook.create_alert(self._build_opsgenie_payload())


send_opsgenie_notification = OpsgenieNotifier
