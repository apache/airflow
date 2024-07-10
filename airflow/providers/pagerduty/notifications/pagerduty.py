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
from typing import Any

from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.pagerduty.hooks.pagerduty_events import PagerdutyEventsHook


class PagerdutyNotifier(BaseNotifier):
    """
    Pagerduty BaseNotifier.

    :param summary: Summary for the event
    :param severity: Severity for the event, needs to be one of: info, warning, error, critical
    :param source: Specific human-readable unique identifier, such as a
        hostname, for the system having the problem.
    :param action: Event action, needs to be one of: trigger, acknowledge,
        resolve. Default to trigger if not specified.
    :param dedup_key: A string which identifies the alert triggered for the given event.
        Required for the actions acknowledge and resolve.
    :param custom_details: Free-form details from the event. Can be a dictionary or a string.
        If a dictionary is passed it will show up in PagerDuty as a table.
    :param group: A cluster or grouping of sources. For example, sources
        "prod-datapipe-02" and "prod-datapipe-03" might both be part of "prod-datapipe"
    :param component: The part or component of the affected system that is broken.
    :param class_type: The class/type of the event.
    :param images: List of images to include. Each dictionary in the list accepts the following keys:
        `src`: The source (URL) of the image being attached to the incident. This image must be served via
        HTTPS.
        `href`: [Optional] URL to make the image a clickable link.
        `alt`: [Optional] Alternative text for the image.
    :param links: List of links to include. Each dictionary in the list accepts the following keys:
        `href`: URL of the link to be attached.
        `text`: [Optional] Plain text that describes the purpose of the link, and can be used as the
        link's text.
    :param integration_key: PagerDuty Events API token
    :param pagerduty_events_conn_id: connection that has PagerDuty integration key in the Pagerduty
     API token field
    """

    template_fields = (
        "summary",
        "severity",
        "source",
        "action",
        "dedup_key",
        "custom_details",
        "group",
        "component",
        "class_type",
        "images",
        "links",
    )

    def __init__(
        self,
        *,
        summary: str,
        severity: str,
        source: str = "airflow",
        action: str = "trigger",
        dedup_key: str | None = None,
        custom_details: Any | None = None,
        group: str | None = None,
        component: str | None = None,
        class_type: str | None = None,
        images: list[Any] | None = None,
        links: list[Any] | None = None,
        pagerduty_events_conn_id: str | None = "pagerduty_events_default",
        integration_key: str | None = None,
    ):
        super().__init__()
        self.pagerduty_events_conn_id = pagerduty_events_conn_id
        self.integration_key = integration_key
        self.summary = summary
        self.severity = severity
        self.source = source
        self.action = action
        self.dedup_key = dedup_key
        self.custom_details = custom_details
        self.group = group
        self.component = component
        self.class_type = class_type
        self.images = images
        self.links = links

    @cached_property
    def hook(self) -> PagerdutyEventsHook:
        """Pagerduty Events Hook."""
        return PagerdutyEventsHook(
            pagerduty_events_conn_id=self.pagerduty_events_conn_id, integration_key=self.integration_key
        )

    def notify(self, context):
        """Send a alert to a pagerduty event v2 API."""
        self.hook.create_event(
            summary=self.summary,
            severity=self.severity,
            source=self.source,
            action=self.action,
            dedup_key=self.dedup_key,
            custom_details=self.custom_details,
            group=self.group,
            component=self.component,
            class_type=self.class_type,
            images=self.images,
            links=self.links,
        )


send_pagerduty_notification = PagerdutyNotifier
