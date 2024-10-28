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

from typing import Any

from opsgenie_sdk import (
    AlertApi,
    ApiClient,
    CloseAlertPayload,
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

    """

    conn_name_attr = "opsgenie_conn_id"
    default_conn_name = "opsgenie_default"
    conn_type = "opsgenie"
    hook_name = "Opsgenie"

    def __init__(self, opsgenie_conn_id: str = "opsgenie_default") -> None:
        super().__init__()  # type: ignore[misc]
        self.conn_id = opsgenie_conn_id
        configuration = Configuration()
        conn = self.get_connection(self.conn_id)
        configuration.api_key["Authorization"] = conn.password
        configuration.host = conn.host or "https://api.opsgenie.com"
        self.alert_api_instance = AlertApi(ApiClient(configuration))

    def _get_api_key(self) -> str:
        """
        Get the API key from the connection.

        :return: API key
        """
        conn = self.get_connection(self.conn_id)
        return conn.password

    def get_conn(self) -> AlertApi:
        """
        Get the underlying AlertApi client.

        :return: AlertApi client
        """
        return self.alert_api_instance

    def create_alert(self, payload: dict | None = None) -> SuccessResponse:
        """
        Create an alert on Opsgenie.

        :param payload: Opsgenie API Create Alert payload values
            See https://docs.opsgenie.com/docs/alert-api#section-create-alert
        :return: api response
        """
        payload = payload or {}

        try:
            create_alert_payload = CreateAlertPayload(**payload)
            api_response = self.alert_api_instance.create_alert(create_alert_payload)
            return api_response
        except OpenApiException as e:
            self.log.exception(
                "Exception when sending alert to opsgenie with payload: %s", payload
            )
            raise e

    def close_alert(
        self,
        identifier: str,
        identifier_type: str | None = "id",
        payload: dict | None = None,
        **kwargs: dict | None,
    ) -> SuccessResponse:
        """
        Close an alert in Opsgenie.

        :param identifier: Identifier of alert which could be alert id, tiny id or alert alias
        :param identifier_type: Type of the identifier that is provided as an in-line parameter.
            Possible values are 'id', 'alias' or 'tiny'
        :param payload: Request payload of closing alert action.
            see https://github.com/opsgenie/opsgenie-python-sdk/blob/master/docs/AlertApi.md#close_alert
        :param kwargs: params to pass to the function
        :return: SuccessResponse
                 If the method is called asynchronously,
                 returns the request thread.
        """
        payload = payload or {}
        try:
            close_alert_payload = CloseAlertPayload(**payload)
            api_response = self.alert_api_instance.close_alert(
                identifier=identifier,
                identifier_type=identifier_type,
                close_alert_payload=close_alert_payload,
                **kwargs,
            )
            return api_response
        except OpenApiException as e:
            self.log.exception(
                "Exception when closing alert in opsgenie with payload: %s", payload
            )
            raise e

    def delete_alert(
        self,
        identifier: str,
        identifier_type: str | None = None,
        user: str | None = None,
        source: str | None = None,
    ) -> SuccessResponse:
        """
        Delete an alert in Opsgenie.

        :param identifier: Identifier of alert which could be alert id, tiny id or alert alias.
        :param identifier_type: Type of the identifier that is provided as an in-line parameter.
            Possible values are 'id', 'alias' or 'tiny'
        :param user: Display name of the request owner.
        :param source: Display name of the request source
        :return: SuccessResponse
        """
        try:
            api_response = self.alert_api_instance.delete_alert(
                identifier=identifier,
                identifier_type=identifier_type,
                user=user,
                source=source,
            )
            return api_response
        except OpenApiException as e:
            self.log.exception("Exception when calling AlertApi->delete_alert: %s\n", e)
            raise e

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Opsgenie connection."""
        return {
            "hidden_fields": ["port", "schema", "login", "extra"],
            "relabeling": {"password": "Opsgenie API Key"},
        }
