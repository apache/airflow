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

from typing import Any, NamedTuple

from airflow.providers.common.compat.sdk import BaseHook


class AccessKeyCredentials(NamedTuple):
    """
    A NamedTuple to store Alibaba Cloud Access Key credentials.

    :param access_key_id: The Access Key ID for Alibaba Cloud authentication.
    :param access_key_secret: The Access Key Secret for Alibaba Cloud authentication.
    """

    access_key_id: str
    access_key_secret: str


class AlibabaBaseHook(BaseHook):
    """
    A base hook for Alibaba Cloud-related hooks.

    This hook provides a common interface for authenticating using Alibaba Cloud credentials.

    Supports Access Key-based authentication.

    :param alibaba_cloud_conn_id: The connection ID to use when fetching connection info.
    """

    conn_name_attr = "alibabacloud_conn_id"
    default_conn_name = "alibabacloud_default"
    conn_type = "alibaba_cloud"
    hook_name = "Alibaba Cloud"

    def __init__(
        self,
        alibabacloud_conn_id: str = "alibabacloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.alibaba_cloud_conn_id = alibabacloud_conn_id
        self.extras: dict = self.get_connection(self.alibaba_cloud_conn_id).extra_dejson

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField

        return {
            "access_key_id": PasswordField(lazy_gettext("Access Key ID"), widget=BS3PasswordFieldWidget()),
            "access_key_secret": PasswordField(
                lazy_gettext("Access Key Secret"), widget=BS3PasswordFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return super().get_ui_field_behaviour()

    def _get_field(self, field_name: str, default: Any = None) -> Any:
        """Fetch a field from extras, and returns it."""
        value = self.extras.get(field_name)
        return value if value is not None else default

    def get_access_key_credential(self) -> AccessKeyCredentials:
        """
        Fetch Access Key Credential for authentication.

        :return: AccessKeyCredentials object containing access_key_id and access_key_secret.
        """
        access_key_id = self._get_field("access_key_id", None)
        access_key_secret = self._get_field("access_key_secret", None)
        if not access_key_id:
            raise ValueError("No access_key_id is specified.")

        if not access_key_secret:
            raise ValueError("No access_key_secret is specified.")

        return AccessKeyCredentials(access_key_id, access_key_secret)
