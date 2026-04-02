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

import pagerduty

from airflow.providers.common.compat.sdk import AirflowException, BaseHook


class PagerdutyHook(BaseHook):
    """
    The PagerdutyHook can be used to interact with both the PagerDuty API and the PagerDuty Events API.

    Takes both PagerDuty API token directly and connection that has PagerDuty API token.
    If both supplied, PagerDuty API token will be used.
    In these cases, the PagerDuty API token refers to an account token:
    https://support.pagerduty.com/docs/generating-api-keys#generating-a-general-access-rest-api-key
    https://support.pagerduty.com/docs/generating-api-keys#generating-a-personal-rest-api-key

    In order to send events (with the Pagerduty Events API), you will also need to specify the
    routing_key (or Integration key) in the ``extra`` field

    :param token: PagerDuty API token
    :param pagerduty_conn_id: connection that has PagerDuty API token in the password field
    """

    conn_name_attr = "pagerduty_conn_id"
    default_conn_name = "pagerduty_default"
    conn_type = "pagerduty"
    hook_name = "Pagerduty"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["port", "login", "schema", "host", "extra"],
            "relabeling": {
                "password": "Pagerduty API token",
            },
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField
        from wtforms.validators import Optional

        return {
            "routing_key": PasswordField(
                lazy_gettext("Routing Key"),
                widget=BS3PasswordFieldWidget(),
                validators=[Optional()],
                default=None,
            ),
        }

    def __init__(self, token: str = "", pagerduty_conn_id: str | None = None) -> None:
        super().__init__()
        self.routing_key = None
        self.token = ""
        self._client: pagerduty.RestApiV2Client | None = None

        if pagerduty_conn_id is not None:
            conn = self.get_connection(pagerduty_conn_id)
            password = conn.password
            if password is not None:
                self.token = password

            routing_key = conn.extra_dejson.get("routing_key")
            if routing_key:
                self.routing_key = routing_key

        if token != "":  # token takes higher priority
            self.token = token

        if self.token == "":
            raise AirflowException("Cannot get token: No valid api token nor pagerduty_conn_id supplied.")

    def client(self) -> pagerduty.RestApiV2Client:
        """
        Return `pagerduty.RestApiV2Client` for use with sending or receiving data through the PagerDuty REST API.

        Documentation on how to use the `RestApiV2Client` class can be found at:
        https://pagerduty.github.io/python-pagerduty/user_guide.html#generic-client-features
        """
        self._client = pagerduty.RestApiV2Client(self.token)
        return self._client

    def test_connection(self):
        try:
            client = pagerduty.RestApiV2Client(self.token)
            client.list_all("services", params={"query": "some_non_existing_service"})
        except Exception:
            return False, "connection test failed, invalid token"
        return True, "connection tested successfully"
