# -*- coding: utf-8 -*-
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
"""Hook for creating Pagerduty incidents."""
from typing import Any, Dict, List, Optional

import pypd

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class PagerdutyHook(BaseHook):
    """
    Takes both Pagerduty API token directly and connection that has Pagerduty API token.

    If both supplied, Pagerduty API token will be used.

    :param token: Pagerduty API token
    :param pagerduty_conn_id: connection that has Pagerduty API token in the password field
    """

    def __init__(self, token: Optional[str] = None, pagerduty_conn_id: Optional[str] = None) -> None:
        self.routing_key = None

        if pagerduty_conn_id is not None:
            conn = self.get_connection(pagerduty_conn_id)
            self.token = conn.get_password()

            routing_key = conn.extra_dejson.get("routing_key")
            if routing_key:
                self.routing_key = routing_key

        if token is not None:  # token takes higher priority
            self.token = token

        if self.token is None:
            raise AirflowException(
                'Cannot get token: No valid api token nor pagerduty_conn_id supplied.')

    # pylint: disable=too-many-arguments
    def create_event(
        self,
        summary: str,
        severity: str,
        source: str = 'airflow',
        action: str = 'trigger',
        routing_key: Optional[str] = None,
        dedup_key: Optional[str] = None,
        custom_details: Optional[Any] = None,
        group: Optional[str] = None,
        component: Optional[str] = None,
        class_type: Optional[str] = None,
        links: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Create event for service integration.

        :param summary: Summary for the event
        :type summary: str
        :param severity: Severity for the event, needs to be one of: Info, Warning, Error, Critical
        :type severity: str
        :param source: Specific human-readable unique identifier, such as a
            hostname, for the system having the problem.
        :type source: str
        :param action: Event action, needs to be one of: trigger, acknowledge,
            resolve. Default to trigger if not specified.
        :type action: str
        :param routing_key: Integration key. If not specified, will try to read
            from connection's extra json blob.
        :type routing_key: str
        :param dedup_key: A string which identifies the alert triggered for the given event
        :type dedup_key: str
        :param custom_details: Free-form details from the event
        :type custom_details: str
        :param group: A cluster or grouping of sources. For example, sources
            “prod-datapipe-02” and “prod-datapipe-03” might both be part of “prod-datapipe”
        :type group: str
        :param component: The part or component of the affected system that is broken.
        :type component: str
        :param class_type: The class/type of the event.
        :type class_type: str
        :param links: List of links to include.
        :type class_type: list of str
        """
        if routing_key is None:
            routing_key = self.routing_key
        if routing_key is None:
            raise AirflowException('No routing/integration key specified.')
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

        data = {
            "routing_key": routing_key,
            "event_action": action,
            "payload": payload,
        }
        if dedup_key:
            data["dedup_key"] = dedup_key
        if links is not None:
            data["links"] = links
        return pypd.EventV2.create(api_key=self.token, data=data)
