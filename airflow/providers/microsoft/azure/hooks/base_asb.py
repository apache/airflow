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

from typing import Any, Dict

from airflow.hooks.base import BaseHook


class BaseAzureServiceBusHook(BaseHook):
    """
    BaseAzureServiceBusHook class to session creation and  connection creation.

    Client ID and Secrete ID's are optional
    :param azure_service_bus_conn_id: Reference to the
        :ref:`Azure Service Bus connection<howto/connection:azure_service_bus>`.
    """

    conn_name_attr = 'azure_service_bus_conn_id'
    default_conn_name = 'azure_service_bus_default'
    conn_type = 'azure_service_bus'
    hook_name = 'Azure ServiceBus'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_service_bus__connection_string": StringField(
                lazy_gettext('Service Bus Connection String'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'host', 'extra'],
            "relabeling": {
                'login': 'Client ID',
                'password': 'Secret',
            },
            "placeholders": {
                'login': 'Client ID (Optional)',
                'password': 'Client Secret (Optional)',
                'extra__azure_service_bus__connection_string': 'Service Bus Connection String',
            },
        }

    def __init__(self, azure_service_bus_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_service_bus_conn_id
        self._conn = None
        self.connection_string = None

    def get_conn(self):
        return None
