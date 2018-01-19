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
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.datadog_hook import DatadogHook
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from datadog import api


class DatadogSensor(BaseSensorOperator):
    """
    A sensor to listen, with a filter, to datadog event streams and determine
    if some event was emitted.

    Depends on the datadog API, which has to be deployed on the same server where
    Airflow runs.

    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    :param datadog_conn_id: string
    """
    ui_color = '#66c3dd'

    @apply_defaults
    def __init__(
            self,
            datadog_conn_id='datadog_default',
            from_seconds_ago=3600,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
            *args,
            **kwargs):
        super(DatadogSensor, self).__init__(*args, **kwargs)
        self.datadog_conn_id = datadog_conn_id
        self.from_seconds_ago = from_seconds_ago
        self.up_to_seconds_from_now = up_to_seconds_from_now
        self.priority = priority
        self.sources = sources
        self.tags = tags
        self.response_check = response_check

    def poke(self, context):
        # This instantiates the hook, but doesn't need it further,
        # because the API authenticates globally (unfortunately),
        # but for airflow this shouldn't matter too much, because each
        # task instance runs in its own process anyway.
        DatadogHook(datadog_conn_id=self.datadog_conn_id)

        response = api.Event.query(
            start=self.from_seconds_ago,
            end=self.up_to_seconds_from_now,
            priority=self.priority,
            sources=self.sources,
            tags=self.tags)

        if isinstance(response, dict) and response.get('status', 'ok') != 'ok':
            self.log.error("Unexpected Datadog result: %s", response)
            raise AirflowException("Datadog returned unexpected result")

        if self.response_check:
            # run content check on response
            return self.response_check(response)

        # If no check was inserted, assume any event that matched yields true.
        return len(response) > 0
