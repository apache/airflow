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

import json
import warnings
from typing import Any, Dict, Optional, Union

import yandexcloud

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class YandexCloudBaseHook(BaseHook):
    """
    A base hook for Yandex.Cloud related tasks.

    :param connection_id: The connection ID to use when fetching connection info.
    :type connection_id: str
    """

    conn_name_attr = 'yandex_conn_id'
    default_conn_name = 'yandexcloud_default'
    conn_type = 'yandexcloud'
    hook_name = 'Yandex Cloud'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "extra__yandexcloud__service_account_json": PasswordField(
                lazy_gettext('Service account auth JSON'),
                widget=BS3PasswordFieldWidget(),
                description='Service account auth JSON. Looks like '
                '{"id", "...", "service_account_id": "...", "private_key": "..."}. '
                'Will be used instead of OAuth token and SA JSON file path field if specified.',
            ),
            "extra__yandexcloud__service_account_json_path": StringField(
                lazy_gettext('Service account auth JSON file path'),
                widget=BS3TextFieldWidget(),
                description='Service account auth JSON file path. File content looks like '
                '{"id", "...", "service_account_id": "...", "private_key": "..."}. '
                'Will be used instead of OAuth token if specified.',
            ),
            "extra__yandexcloud__oauth": PasswordField(
                lazy_gettext('OAuth Token'),
                widget=BS3PasswordFieldWidget(),
                description='User account OAuth token. '
                'Either this or service account JSON must be specified.',
            ),
            "extra__yandexcloud__folder_id": StringField(
                lazy_gettext('Default folder ID'),
                widget=BS3TextFieldWidget(),
                description='Optional. This folder will be used '
                'to create all new clusters and nodes by default',
            ),
            "extra__yandexcloud__public_ssh_key": StringField(
                lazy_gettext('Public SSH key'),
                widget=BS3TextFieldWidget(),
                description='Optional. This key will be placed to all created Compute nodes'
                'to let you have a root shell there',
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "relabeling": {},
        }

    def __init__(
        self,
        # Connection id is deprecated. Use yandex_conn_id instead
        connection_id: Optional[str] = None,
        yandex_conn_id: Optional[str] = None,
        default_folder_id: Union[dict, bool, None] = None,
        default_public_ssh_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        if connection_id:
            warnings.warn(
                "Using `connection_id` is deprecated. Please use `yandex_conn_id` parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.connection_id = yandex_conn_id or connection_id or self.default_conn_name
        self.connection = self.get_connection(self.connection_id)
        self.extras = self.connection.extra_dejson
        credentials = self._get_credentials()
        self.sdk = yandexcloud.SDK(**credentials)
        self.default_folder_id = default_folder_id or self._get_field('folder_id', False)
        self.default_public_ssh_key = default_public_ssh_key or self._get_field('public_ssh_key', False)
        self.client = self.sdk.client

    def _get_credentials(self) -> Dict[str, Any]:
        service_account_json_path = self._get_field('service_account_json_path', False)
        service_account_json = self._get_field('service_account_json', False)
        oauth_token = self._get_field('oauth', False)
        if not (service_account_json or oauth_token or service_account_json_path):
            raise AirflowException(
                'No credentials are found in connection. Specify either service account '
                + 'authentication JSON or user OAuth token in Yandex.Cloud connection'
            )
        if service_account_json_path:
            with open(service_account_json_path) as infile:
                service_account_json = infile.read()
        if service_account_json:
            service_account_key = json.loads(service_account_json)
            return {'service_account_key': service_account_key}
        else:
            return {'token': oauth_token}

    def _get_field(self, field_name: str, default: Any = None) -> Any:
        """Fetches a field from extras, and returns it."""
        long_f = f'extra__yandexcloud__{field_name}'
        if hasattr(self, 'extras') and long_f in self.extras:
            return self.extras[long_f]
        else:
            return default
