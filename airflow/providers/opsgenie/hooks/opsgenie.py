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
#

from typing import Optional

from opsgenie_sdk import (
    AlertApi,
    ApiClient,
    Configuration,
    CreateAlertPayload,
    OpenApiException,
    SuccessResponse,
)

from airflow.hooks.base import BaseHook


class OpsgenieAlertHook(BaseHook):
    """
    This hook allows you to post alerts to Opsgenie.
    Accepts a connection that has an Opsgenie API key as the connection's password.
    This hook sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this hook.

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :type opsgenie_conn_id: str

    """

    conn_name_attr = 'opsgenie_conn_id'
    default_conn_name = 'opsgenie_default'
    conn_type = 'opsgenie'
    hook_name = 'Opsgenie'

    def __init__(self, opsgenie_conn_id: str = 'opsgenie_default') -> None:
        super().__init__()  # type: ignore[misc]
        self.conn_id = opsgenie_conn_id
        configuration = Configuration()
        conn = self.get_connection(self.conn_id)
        configuration.api_key['Authorization'] = conn.password
        configuration.host = conn.host or 'https://api.opsgenie.com'
        self.alert_api_instance = AlertApi(ApiClient(configuration))

    def _get_api_key(self) -> str:
        """
        Get the API key from the connection

        :return: API key
        :rtype: str
        """
        conn = self.get_connection(self.conn_id)
        return conn.password

    def get_conn(self) -> AlertApi:
        """
        Get the underlying AlertApi client

        :return: AlertApi client
        :rtype: opsgenie_sdk.AlertApi
        """
        return self.alert_api_instance

    def create_alert(self, payload: Optional[dict] = None) -> SuccessResponse:
        """
        Create an alert on Opsgenie

        :param payload: Opsgenie API Create Alert payload values
            See https://docs.opsgenie.com/docs/alert-api#section-create-alert
        :type payload: dict
        :return: api response
        :rtype: opsgenie_sdk.SuccessResponse
        """
        payload = payload or {}

        try:
            create_alert_payload = CreateAlertPayload(**payload)
            api_response = self.alert_api_instance.create_alert(create_alert_payload)
            return api_response
        except OpenApiException as e:
            self.log.exception('Exception when sending alert to opsgenie with payload: %s', payload)
            raise e
