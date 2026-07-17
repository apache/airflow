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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.opsgenie.hooks.opsgenie import OpsgenieAlertHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class OpsgenieCreateAlertOperator(BaseOperator):
    """
    This operator allows you to post alerts to Opsgenie.

    Accepts a connection that has an Opsgenie API key as the connection's password.
    This operator sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this operator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpsgenieCreateAlertOperator`

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

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

    def execute(self, context: Context) -> None:
        """Call the OpsgenieAlertHook to post message."""
        self.hook = OpsgenieAlertHook(self.opsgenie_conn_id)
        self.hook.create_alert(self._build_opsgenie_payload())


class OpsgenieCloseAlertOperator(BaseOperator):
    """
    This operator allows you to close alerts to Opsgenie.

    Accepts a connection that has an Opsgenie API key as the connection's password.
    This operator sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this operator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpsgenieCloseAlertOperator`

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :param identifier: Identifier of alert which could be alert id, tiny id or alert alias
    :param identifier_type: Type of the identifier that is provided as an in-line parameter.
        Possible values are 'id', 'alias' or 'tiny'
    :param user: display name of the request owner
    :param note: additional note that will be added while creating the alert
    :param source: source field of the alert. Default value is IP address of the incoming request
    :param close_alert_kwargs: additional params to pass
    """

    def __init__(
        self,
        *,
        identifier: str,
        opsgenie_conn_id: str = "opsgenie_default",
        identifier_type: str | None = None,
        user: str | None = None,
        note: str | None = None,
        source: str | None = None,
        close_alert_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.opsgenie_conn_id = opsgenie_conn_id
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.user = user
        self.note = note
        self.source = source
        self.close_alert_kwargs = close_alert_kwargs
        self.hook: OpsgenieAlertHook | None = None

    def _build_opsgenie_close_alert_payload(self) -> dict[str, Any]:
        """
        Construct the Opsgenie JSON payload.

        All relevant parameters are combined here to a valid Opsgenie JSON payload.

        :return: Opsgenie close alert payload (dict) to send
        """
        payload = {}

        for key in [
            "user",
            "note",
            "source",
        ]:
            val = getattr(self, key)
            if val:
                payload[key] = val
        return payload

    def execute(self, context: Context) -> None:
        """Call the OpsgenieAlertHook to close alert."""
        self.hook = OpsgenieAlertHook(self.opsgenie_conn_id)
        self.hook.close_alert(
            identifier=self.identifier,
            identifier_type=self.identifier_type,
            payload=self._build_opsgenie_close_alert_payload(),
            **(self.close_alert_kwargs or {}),
        )


class OpsgenieDeleteAlertOperator(BaseOperator):
    """
    This operator allows you to delete alerts in Opsgenie.

    Accepts a connection that has an Opsgenie API key as the connection's password.
    This operator sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this operator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpsgenieDeleteAlertOperator`

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :param identifier: Identifier of alert which could be alert id, tiny id or alert alias
    :param identifier_type: Type of the identifier that is provided as an in-line parameter.
        Possible values are 'id', 'alias' or 'tiny'
    :param user: Display name of the request owner
    :param source: Display name of the request source
    """

    template_fields: Sequence[str] = ("identifier",)

    def __init__(
        self,
        *,
        identifier: str,
        opsgenie_conn_id: str = "opsgenie_default",
        identifier_type: str | None = None,
        user: str | None = None,
        source: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.opsgenie_conn_id = opsgenie_conn_id
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.user = user
        self.source = source

    def execute(self, context: Context) -> None:
        """Call the OpsgenieAlertHook to delete alert."""
        hook = OpsgenieAlertHook(self.opsgenie_conn_id)
        hook.delete_alert(
            identifier=self.identifier,
            identifier_type=self.identifier_type,
            user=self.user,
            source=self.source,
        )
