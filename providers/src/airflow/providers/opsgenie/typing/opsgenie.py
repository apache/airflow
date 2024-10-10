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

from typing import TypedDict

from typing_extensions import NotRequired, Required  # For compat with Python < 3.11


class CreateAlertPayload(TypedDict):
    """
    Payload schema for creating an Opsgenie alert.

    :param message: The Message of the Opsgenie alert.
    :param alias: Client-defined identifier of the alert.
    :param description: Description field of the alert.
    :param responders: Teams, users, escalations and schedules that
        the alert will be routed to send notifications.
    :param visible_to: Teams and users that the alert will become visible
        to without sending any notification.
    :param actions: Custom actions that will be available for the alert.
    :param tags: Tags of the alert.
    :param details: Map of key-value pairs to use as custom properties of the alert.
    :param entity: Entity field of the alert that is
        generally used to specify which domain alert is related to.
    :param source: Source field of the alert. Default value is
        IP address of the incoming request.
    :param priority: Priority level of the alert. Default value is P3.
    :param user: Display name of the request owner.
    :param note: Additional note that will be added while creating the alert.
    """

    message: Required[str]
    alias: NotRequired[str | None]
    description: NotRequired[str | None]
    responders: NotRequired[list[dict] | None]
    visible_to: NotRequired[list[dict] | None]
    actions: NotRequired[list[str] | None]
    tags: NotRequired[list[str] | None]
    details: NotRequired[dict | None]
    entity: NotRequired[str | None]
    source: NotRequired[str | None]
    priority: NotRequired[str | None]
    user: NotRequired[str | None]
    note: NotRequired[str | None]
