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

import time
from typing import Any

from datadog import api, initialize  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DatadogHook(BaseHook, LoggingMixin):
    """
    Uses datadog API to send metrics of practically anything measurable.

    It's possible to track # of db records inserted/deleted, records read
    from file and many other useful metrics.

    Depends on the datadog API, which has to be deployed on the same server where
    Airflow runs.

    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    """

    conn_name_attr = "datadog_conn_id"
    default_conn_name = "datadog_default"
    conn_type = "datadog"
    hook_name = "Datadog"

    def __init__(self, datadog_conn_id: str = "datadog_default") -> None:
        super().__init__()
        conn = self.get_connection(datadog_conn_id)
        self.api_key = conn.extra_dejson.get("api_key", None)
        self.app_key = conn.extra_dejson.get("app_key", None)
        self.api_host = conn.extra_dejson.get("api_host", None)
        self.source_type_name = conn.extra_dejson.get("source_type_name", None)

        # If the host is populated, it will use that hostname instead.
        # for all metric submissions.
        self.host = conn.host

        if self.api_key is None:
            raise AirflowException("api_key must be specified in the Datadog connection details")

        self.log.info("Setting up api keys for Datadog")
        initialize(api_key=self.api_key, app_key=self.app_key, api_host=self.api_host)

    def validate_response(self, response: dict[str, Any]) -> None:
        """Validate Datadog response."""
        if response["status"] != "ok":
            self.log.error("Datadog returned: %s", response)
            raise AirflowException("Error status received from Datadog")

    def send_metric(
        self,
        metric_name: str,
        datapoint: float | int,
        tags: list[str] | None = None,
        type_: str | None = None,
        interval: int | None = None,
    ) -> dict[str, Any]:
        """
        Send a single datapoint metric to Datadog.

        :param metric_name: The name of the metric
        :param datapoint: A single integer or float related to the metric
        :param tags: A list of tags associated with the metric
        :param type_: Type of your metric: gauge, rate, or count
        :param interval: If the type of the metric is rate or count, define the corresponding interval
        """
        response = api.Metric.send(
            metric=metric_name, points=datapoint, host=self.host, tags=tags, type=type_, interval=interval
        )

        self.validate_response(response)
        return response

    def query_metric(self, query: str, from_seconds_ago: int, to_seconds_ago: int) -> dict[str, Any]:
        """
        Query datadog for a metric, potentially with some function applied to it and return the result.

        :param query: The datadog query to execute (see datadog docs)
        :param from_seconds_ago: How many seconds ago to start querying for.
        :param to_seconds_ago: Up to how many seconds ago to query for.
        """
        now = int(time.time())

        response = api.Metric.query(start=now - from_seconds_ago, end=now - to_seconds_ago, query=query)

        self.validate_response(response)
        return response

    def post_event(
        self,
        title: str,
        text: str,
        aggregation_key: str | None = None,
        alert_type: str | None = None,
        date_happened: int | None = None,
        handle: str | None = None,
        priority: str | None = None,
        related_event_id: int | None = None,
        tags: list[str] | None = None,
        device_name: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Post an event to datadog (processing finished, potentially alerts, other issues).

        Think about this as a means to maintain persistence of alerts, rather than alerting itself.

        :param title: The title of the event
        :param text: The body of the event (more information)
        :param aggregation_key: Key that can be used to aggregate this event in a stream
        :param alert_type: The alert type for the event, one of
            ["error", "warning", "info", "success"]
        :param date_happened: POSIX timestamp of the event; defaults to now
        :handle: User to post the event as; defaults to owner of the application key used
            to submit.
        :param handle: str
        :param priority: Priority to post the event as. ("normal" or "low", defaults to "normal")
        :param related_event_id: Post event as a child of the given event
        :param tags: List of tags to apply to the event
        :param device_name: device_name to post the event with
        """
        response = api.Event.create(
            title=title,
            text=text,
            aggregation_key=aggregation_key,
            alert_type=alert_type,
            date_happened=date_happened,
            handle=handle,
            priority=priority,
            related_event_id=related_event_id,
            tags=tags,
            host=self.host,
            device_name=device_name,
            source_type_name=self.source_type_name,
        )

        self.validate_response(response)
        return response

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "api_host": StringField(lazy_gettext("API endpoint"), widget=BS3TextFieldWidget()),
            "api_key": StringField(lazy_gettext("API key"), widget=BS3TextFieldWidget()),
            "app_key": StringField(lazy_gettext("Application key"), widget=BS3TextFieldWidget()),
            "source_type_name": StringField(lazy_gettext("Source type name"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "login", "password", "port", "extra"],
            "relabeling": {"host": "Events host name"},
        }
