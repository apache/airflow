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
"""Whether a webhook can be served, as ``airflow webhooks check`` and ``GET /api/v2/webhooks`` report it."""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import TYPE_CHECKING

from airflow.models.connection import Connection
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.webhooks.connections import (
    CONN_ID_PREFIX,
    MISCONFIGURATIONS,
    ConfiguredWebhook,
    ResolutionFailure,
    WebhookSourceTypeStatus,
    resolve_webhook,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.webhooks.registry import WebhookRegistry


class WebhookConnectionStatus(str, enum.Enum):
    """Whether a webhook's ``webhook_<name>`` connection configures a webhook that can be served."""

    OK = "ok"
    #: No secrets backend has the connection.
    NOT_FOUND = "not_found"
    #: The connection's type is not ``inbound_webhook``.
    WRONG_TYPE = "wrong_type"
    #: The connection cannot be used as it is, such as one without a password, of a team, or naming a
    #: source type that no installed provider serves.
    MISCONFIGURED = "misconfigured"
    #: A secrets backend failed, so whether the connection exists is not known.
    UNAVAILABLE = "unavailable"


_CONNECTION_STATUSES = {
    ResolutionFailure.NOT_FOUND: WebhookConnectionStatus.NOT_FOUND,
    ResolutionFailure.UNAVAILABLE: WebhookConnectionStatus.UNAVAILABLE,
    ResolutionFailure.WRONG_TYPE: WebhookConnectionStatus.WRONG_TYPE,
    ResolutionFailure.MISCONFIGURED: WebhookConnectionStatus.MISCONFIGURED,
}


@dataclass(frozen=True, kw_only=True)
class WebhookStatus:
    """
    Whether a webhook can be served by ``airflow api-server --apps webhooks``, and if not, why.

    :param name: The webhook's name.
    :param listed: Whether ``[webhooks] names`` lists it, which it must to be served.
    :param connection_status: Whether its connection configures a webhook that can be served.
    :param source_type: The source type its connection names, or None if the connection was not read
        that far.
    :param source_type_status: Whether an installed provider serves ``source_type``, or None if it was
        not looked up.
    :param webhook: The webhook as its connection configures it, or None if it does not.
    :param problems: What keeps the webhook from being served, for the Deployment Manager. A problem can
        quote a secrets backend's error, so the REST API never returns them.
    """

    name: str
    listed: bool
    connection_status: WebhookConnectionStatus
    source_type: str | None
    source_type_status: WebhookSourceTypeStatus | None
    webhook: ConfiguredWebhook | None
    problems: tuple[str, ...]


@provide_session
def get_webhook_status(
    name: str, *, registry: WebhookRegistry, session: Session = NEW_SESSION
) -> WebhookStatus:
    """
    Return whether webhook ``name`` can be served, resolving its connection as the webhooks app does.

    The connection is read from the secrets backends without a cache, so each call reads it again.

    :param name: A webhook name; ``get_webhook_name_problem`` says whether a string is one.
    :param registry: The ``[webhooks]`` configuration.
    :param session: The session to look up a connection's team with.
    """
    problems = []
    listed = name in registry.names
    if not listed:
        problems.append("[webhooks] names does not list it, so its deliveries are answered with 404")
    resolution = resolve_webhook(
        name,
        source_type_owners=registry.source_type_owners,
        default_max_body_bytes=registry.max_body_bytes,
        default_store_payload=registry.store_payload,
    )
    if isinstance(resolution, ConfiguredWebhook):
        return WebhookStatus(
            name=name,
            listed=listed,
            connection_status=WebhookConnectionStatus.OK,
            source_type=resolution.source_type,
            source_type_status=WebhookSourceTypeStatus.OK,
            webhook=resolution,
            problems=tuple(problems),
        )
    connection_status = _CONNECTION_STATUSES[resolution.failure]
    conn_id = f"{CONN_ID_PREFIX}{name}"
    if resolution.failure in MISCONFIGURATIONS:
        problems.append(f"connection {conn_id} is misconfigured: {resolution.reason}")
    elif resolution.failure is ResolutionFailure.NOT_FOUND and (
        team_name := Connection.get_team_name(conn_id, session=session)
    ):
        # resolve_webhook looks connections up without a team, so it does not find one of a team.
        connection_status = WebhookConnectionStatus.MISCONFIGURED
        problems.append(
            f"connection {conn_id} belongs to team {team_name}, and this version of Airflow serves only "
            "webhooks without a team"
        )
    else:
        problems.append(resolution.reason)
    return WebhookStatus(
        name=name,
        listed=listed,
        connection_status=connection_status,
        source_type=resolution.source_type,
        source_type_status=resolution.source_type_status,
        webhook=None,
        problems=tuple(problems),
    )
