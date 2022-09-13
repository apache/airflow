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

from azure.common.client_factory import get_client_from_auth_file, get_client_from_json_dict
from azure.common.credentials import ServicePrincipalCredentials

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AzureBaseHook(BaseHook):
    """
    This hook acts as a base hook for azure services. It offers several authentication mechanisms to
    authenticate the client library used for upstream azure hooks.

    :param sdk_client: The SDKClient to use.
    :param conn_id: The :ref:`Azure connection id<howto/connection:azure>`
        which refers to the information to connect to the service.
    """

    conn_name_attr = 'azure_conn_id'
    default_conn_name = 'azure_default'
    conn_type = 'azure'
    hook_name = 'Azure'

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure__tenantId": StringField(
                lazy_gettext('Azure Tenant ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure__subscriptionId": StringField(
                lazy_gettext('Azure Subscription ID'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ['schema', 'port', 'host'],
            "relabeling": {
                'login': 'Azure Client ID',
                'password': 'Azure Secret',
            },
            "placeholders": {
                'extra': json.dumps(
                    {
                        "key_path": "path to json file for auth",
                        "key_json": "specifies json dict for auth",
                    },
                    indent=1,
                ),
                'login': 'client_id (token credentials auth)',
                'password': 'secret (token credentials auth)',
                'extra__azure__tenantId': 'tenantId (token credentials auth)',
                'extra__azure__subscriptionId': 'subscriptionId (token credentials auth)',
            },
        }

    def __init__(self, sdk_client: Any, conn_id: str = 'azure_default'):
        self.sdk_client = sdk_client
        self.conn_id = conn_id
        super().__init__()

    def get_conn(self) -> Any:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get('extra__azure__tenantId') or conn.extra_dejson.get('tenantId')
        subscription_id = conn.extra_dejson.get('extra__azure__subscriptionId') or conn.extra_dejson.get(
            'subscriptionId'
        )

        key_path = conn.extra_dejson.get('key_path')
        if key_path:
            if not key_path.endswith('.json'):
                raise AirflowException('Unrecognised extension for key file.')
            self.log.info('Getting connection using a JSON key file.')
            return get_client_from_auth_file(client_class=self.sdk_client, auth_path=key_path)

        key_json = conn.extra_dejson.get('key_json')
        if key_json:
            self.log.info('Getting connection using a JSON config.')
            return get_client_from_json_dict(client_class=self.sdk_client, config_dict=key_json)

        self.log.info('Getting connection using specific credentials and subscription_id.')
        return self.sdk_client(
            credentials=ServicePrincipalCredentials(
                client_id=conn.login, secret=conn.password, tenant=tenant
            ),
            subscription_id=subscription_id,
        )
