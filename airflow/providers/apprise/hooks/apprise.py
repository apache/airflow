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

import json
import warnings
from typing import TYPE_CHECKING, Any, Iterable

import apprise
from apprise import AppriseConfig, NotifyFormat, NotifyType

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from apprise import AppriseAttachment


class AppriseHook(BaseHook):
    """
    Use Apprise(https://github.com/caronc/apprise) to interact with notification services.

    The complete list of notification services supported by Apprise can be found at:
    https://github.com/caronc/apprise/wiki#notification-services.

    :param apprise_conn_id: :ref:`Apprise connection id <howto/connection:apprise>`
        that has services configured in the `config` field.
    """

    conn_name_attr = "apprise_conn_id"
    default_conn_name = "apprise_default"
    conn_type = "apprise"
    hook_name = "Apprise"

    def __init__(self, apprise_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.apprise_conn_id = apprise_conn_id

    def get_config_from_conn(self):
        conn = self.get_connection(self.apprise_conn_id)
        config = conn.extra_dejson["config"]
        return json.loads(config) if isinstance(config, str) else config

    def set_config_from_conn(self, apprise_obj: apprise.Apprise):
        """Set config from connection to apprise object."""
        config_object = self.get_config_from_conn()
        if isinstance(config_object, list):
            for config in config_object:
                apprise_obj.add(config["path"], tag=config.get("tag", None))
        elif isinstance(config_object, dict):
            apprise_obj.add(config_object["path"], tag=config_object.get("tag", None))
        else:
            raise ValueError(
                f"Only types of dict or list[dict] are expected in Apprise connections,"
                f" got {type(config_object)}"
            )

    def notify(
        self,
        body: str,
        title: str | None = None,
        notify_type: NotifyType = NotifyType.INFO,
        body_format: NotifyFormat = NotifyFormat.TEXT,
        tag: str | Iterable[str] | None = None,
        attach: AppriseAttachment | None = None,
        interpret_escapes: bool | None = None,
        config: AppriseConfig | None = None,
    ):
        r"""
        Send message to plugged-in services.

        :param body: Specify the message body
        :param title: Specify the message title. (optional)
        :param notify_type: Specify the message type (default=info). Possible values are "info",
            "success", "failure", and "warning"
        :param body_format: Specify the input message format (default=text). Possible values are "text",
            "html", and "markdown".
        :param tag: Specify one or more tags to filter which services to notify
        :param attach: Specify one or more file attachment locations
        :param interpret_escapes: Enable interpretation of backslash escapes. For example, this would convert
            sequences such as \n and \r to their respective ascii new-line and carriage return characters
        :param config: Specify one or more configuration
        """
        if tag is None:
            warnings.warn(
                "`tag` cannot be None. Assign it to be MATCH_ALL_TAG",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            tag = "all"

        title = title or ""

        apprise_obj = apprise.Apprise()
        if config:
            apprise_obj.add(config)
        else:
            self.set_config_from_conn(apprise_obj)
        apprise_obj.notify(
            body=body,
            title=title,
            notify_type=notify_type,
            body_format=body_format,
            tag=tag,
            attach=attach,
            interpret_escapes=interpret_escapes,
        )

    def get_conn(self) -> None:
        raise NotImplementedError()

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField

        return {
            "config": PasswordField(
                lazy_gettext("config"),
                widget=BS3PasswordFieldWidget(),
                description='format example - {"path": "service url", "tag": "alerts"} or '
                '[{"path": "service url", "tag": "alerts"},'
                ' {"path": "service url", "tag": "alerts"}]',
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }
