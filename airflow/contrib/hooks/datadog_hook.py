# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from datadog import initialize, api

from airflow.utils.log.logging_mixin import LoggingMixin


class DatadogHook(BaseHook, LoggingMixin):
    """
    Uses datadog API to send metrics of practically anything measurable,
    so it's possible to track # of db records inserted/deleted, records read
    from file and many other useful metrics.

    Depends on the datadog API, which has to be deployed on the same server where
    Airflow runs.

    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    :param datadog_conn_id: string
    """
    def __init__(self, datadog_conn_id='datadog_default'):
        conn = self.get_connection(datadog_conn_id)
        self.api_key = conn.extra_dejson.get('api_key', None)
        self.app_key = conn.extra_dejson.get('app_key', None)
        self.source_type_name = conn.extra_dejson.get('source_type_name', None)

        # If the host is populated, it will use that hostname instead.
        # for all metric submissions.
        self.host = conn.host

        if self.api_key is None:
            raise AirflowException("api_key must be specified in the Datadog connection details")
        if self.app_key is None:
            raise AirflowException("app_key must be specified in the Datadog connection details")

        self.log.info("Setting up api keys for Datadog")
        options = {
            'api_key': self.api_key,
            'app_key': self.app_key
        }
        initialize(**options)

    def validate_response(self, response):
        if response['status'] != 'ok':
            self.log.error("Datadog returned: %s", response)
            raise AirflowException("Error status received from Datadog")

    def send_metric(self, metric_name, datapoint, tags=None):
        """
        Sends a single datapoint metric to DataDog

        :param metric_name: The name of the metric
        :type metric_name: string
        :param datapoint: A single integer or float related to the metric
        :type datapoint: integer or float
        :param tags: A list of tags associated with the metric
        :type tags: list
        """
        response = api.Metric.send(
            metric=metric_name,
            points=datapoint,
            host=self.host,
            tags=tags)

        self.validate_response(response)
        return response

    def query_metric(self,
                     query,
                     from_seconds_ago,
                     to_seconds_ago):
        """
        Queries datadog for a specific metric, potentially with some function applied to it
        and returns the results.

        :param query: The datadog query to execute (see datadog docs)
        :type query: string
        :param from_seconds_ago: How many seconds ago to start querying for.
        :type from_seconds_ago: int
        :param to_seconds_ago: Up to how many seconds ago to query for.
        :type to_seconds_ago: int
        """
        now = int(time.time())

        response = api.Metric.query(
            start=now - from_seconds_ago,
            end=now - to_seconds_ago,
            query=query)

        self.validate_response(response)
        return response

    def post_event(self, title, text, tags=None, alert_type=None, aggregation_key=None):
        """
        Posts an event to datadog (processing finished, potentially alerts, other issues)
        Think about this as a means to maintain persistence of alerts, rather than alerting
        itself.

        :param title: The title of the event
        :type title: string
        :param text: The body of the event (more information)
        :type text: string
        :param tags: List of string tags to apply to the event
        :type tags: list
        :param alert_type: The alert type for the event, one of
            ["error", "warning", "info", "success"]
        :type alert_type: string
        :param aggregation_key: Key that can be used to aggregate this event in a stream
        :type aggregation_key: string
        """
        response = api.Event.create(
            title=title,
            text=text,
            host=self.host,
            tags=tags,
            alert_type=alert_type,
            aggregation_key=aggregation_key,
            source_type_name=self.source_type_name)

        self.validate_response(response)
        return response
