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

import warnings
from typing import Any

import yandexcloud

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook
from airflow.providers.yandex.utils.credentials import (
    CredentialsType,
    get_credentials,
    get_service_account_id,
)
from airflow.providers.yandex.utils.defaults import (
    conn_name_attr,
    conn_type,
    default_conn_name,
    hook_name,
)
from airflow.providers.yandex.utils.fields import get_field_from_extras
from airflow.providers.yandex.utils.user_agent import provider_user_agent


class YandexCloudBaseHook(BaseHook):
    """
    A base hook for Yandex.Cloud related tasks.

    :param yandex_conn_id: The connection ID to use when fetching connection info
    :param connection_id: Deprecated, use yandex_conn_id instead
    :param default_folder_id: The folder ID to use instead of connection folder ID
    :param default_public_ssh_key: The key to use instead of connection key
    :param default_service_account_id: The service account ID to use instead of key service account ID
    """

    conn_name_attr = conn_name_attr
    default_conn_name = default_conn_name
    conn_type = conn_type
    hook_name = hook_name

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Yandex connection form."""
        from flask_appbuilder.fieldwidgets import (
            BS3PasswordFieldWidget,
            BS3TextFieldWidget,
        )
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "service_account_json": PasswordField(
                lazy_gettext("Service account auth JSON"),
                widget=BS3PasswordFieldWidget(),
                description="Service account auth JSON. Looks like "
                '{"id": "...", "service_account_id": "...", "private_key": "..."}. '
                "Will be used instead of OAuth token and SA JSON file path field if specified.",
            ),
            "service_account_json_path": StringField(
                lazy_gettext("Service account auth JSON file path"),
                widget=BS3TextFieldWidget(),
                description="Service account auth JSON file path. File content looks like "
                '{"id": "...", "service_account_id": "...", "private_key": "..."}. '
                "Will be used instead of OAuth token if specified.",
            ),
            "oauth": PasswordField(
                lazy_gettext("OAuth Token"),
                widget=BS3PasswordFieldWidget(),
                description="User account OAuth token. "
                "Either this or service account JSON must be specified.",
            ),
            "folder_id": StringField(
                lazy_gettext("Default folder ID"),
                widget=BS3TextFieldWidget(),
                description="Optional. "
                "If specified, this ID will be used by default when creating nodes and clusters.",
            ),
            "public_ssh_key": StringField(
                lazy_gettext("Public SSH key"),
                widget=BS3TextFieldWidget(),
                description="Optional. The key will be placed to all created Compute nodes, "
                "allowing you to have a root shell there.",
            ),
            "endpoint": StringField(
                lazy_gettext("API endpoint"),
                widget=BS3TextFieldWidget(),
                description="Optional. Specify an API endpoint. Leave blank to use default.",
            ),
        }

    @classmethod
    def provider_user_agent(cls) -> str | None:
        warnings.warn(
            "Using `provider_user_agent` in `YandexCloudBaseHook` is deprecated. "
            "Please use it in `utils.user_agent` instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return provider_user_agent()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Yandex connection."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    def __init__(
        self,
        # connection_id is deprecated, use yandex_conn_id instead
        connection_id: str | None = None,
        yandex_conn_id: str | None = None,
        default_folder_id: str | None = None,
        default_public_ssh_key: str | None = None,
        default_service_account_id: str | None = None,
    ) -> None:
        super().__init__()
        if connection_id:
            warnings.warn(
                "Using `connection_id` is deprecated. Please use `yandex_conn_id` parameter.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        self.connection_id = yandex_conn_id or connection_id or default_conn_name
        self.connection = self.get_connection(self.connection_id)
        self.extras = self.connection.extra_dejson
        self.credentials: CredentialsType = get_credentials(
            oauth_token=self._get_field("oauth"),
            service_account_json=self._get_field("service_account_json"),
            service_account_json_path=self._get_field("service_account_json_path"),
        )
        sdk_config = self._get_endpoint()
        self.sdk = yandexcloud.SDK(
            user_agent=provider_user_agent(),
            token=self.credentials.get("token"),
            service_account_key=self.credentials.get("service_account_key"),
            endpoint=sdk_config.get("endpoint"),
        )
        self.default_folder_id = default_folder_id or self._get_field("folder_id")
        self.default_public_ssh_key = default_public_ssh_key or self._get_field(
            "public_ssh_key"
        )
        self.default_service_account_id = (
            default_service_account_id
            or get_service_account_id(
                service_account_json=self._get_field("service_account_json"),
                service_account_json_path=self._get_field("service_account_json_path"),
            )
        )
        self.client = self.sdk.client

    def _get_endpoint(self) -> dict[str, str]:
        sdk_config = {}
        endpoint = self._get_field("endpoint")
        if endpoint:
            sdk_config["endpoint"] = endpoint
        return sdk_config

    def _get_field(self, field_name: str, default: Any = None) -> Any:
        if not hasattr(self, "extras"):
            return default
        return get_field_from_extras(self.extras, field_name, default)
