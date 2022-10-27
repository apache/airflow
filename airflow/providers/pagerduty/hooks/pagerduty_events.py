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
"""Hook for sending or receiving data from PagerDuty as well as creating PagerDuty incidents."""
from __future__ import annotations

from typing import Any

import pdpyras

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class PagerdutyEventsHook(BaseHook):
    """
    This class can be used to interact with the Pagerduty Events API.

    It takes both an Events API token and a PagerDuty connection with the Events API token
     (i.e. Integration key) as the password/Pagerduty API token. If both supplied, the token will be used.

    :param integration_key: PagerDuty Events API token
    :param pagerduty_conn_id: connection that has PagerDuty integration key in the Pagerduty API token field
    """

    conn_name_attr = "pagerduty_events_conn_id"
    default_conn_name = "pagerduty_events_default"
    conn_type = "pagerduty_events"
    hook_name = "Pagerduty Events"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "login", "schema", "host", "extra"],
            "relabeling": {
                "password": "Pagerduty Integration key",
            },
        }

    def __init__(
        self, integration_key: str | None = None, pagerduty_events_conn_id: str | None = None
    ) -> None:
        super().__init__()
        self.integration_key = None
        self._session = None

        if pagerduty_events_conn_id is not None:
            conn = self.get_connection(pagerduty_events_conn_id)
            self.integration_key = conn.get_password()

        if integration_key is not None:  # token takes higher priority
            self.integration_key = integration_key

        if self.integration_key is None:
            raise AirflowException(
                "Cannot get token: No valid integration key nor pagerduty_events_conn_id supplied."
            )

    def create_event(
        self,
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
    ) -> dict:
        """
        Create event for service integration.

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
        :return: PagerDuty Events API v2 response.
        """
        payload = {
            "summary": summary,
            "severity": severity,
            "source": source,
        }
        if custom_details is not None:
            payload["custom_details"] = custom_details
        if component:
            payload["component"] = component
        if group:
            payload["group"] = group
        if class_type:
            payload["class"] = class_type

        actions = ("trigger", "acknowledge", "resolve")
        if action not in actions:
            raise ValueError(f"Event action must be one of: {', '.join(actions)}")
        data = {
            "event_action": action,
            "payload": payload,
        }
        if dedup_key:
            data["dedup_key"] = dedup_key
        elif action != "trigger":
            raise ValueError(
                f"The dedup_key property is required for event_action={action} events, "
                f"and it must be a string."
            )
        if images is not None:
            data["images"] = images
        if links is not None:
            data["links"] = links

        session = pdpyras.EventsAPISession(self.integration_key)
        resp = session.post("/v2/enqueue", json=data)
        resp.raise_for_status()
        return resp.json()
