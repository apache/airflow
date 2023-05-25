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
from typing import Any, Iterable

import apprise
from airflow.hooks.base import BaseHook
from apprise import AppriseConfig, NotifyFormat, NotifyType


class AppriseHook(BaseHook):
    """
    Use Apprise(https://github.com/caronc/apprise) to interact with notification services, the entire list
     of notification services supported by apprise can be found here
      - https://github.com/caronc/apprise#productivity-based-notifications
    """

    conn_name_attr = "apprise_conn_id"
    default_conn_name = "apprise_default"
    conn_type = "apprise"
    hook_name = "Apprise"

    def __init__(self, apprise_conn_id: str = default_conn_name):
        super().__init__()
        self.apprise_conn_id = apprise_conn_id

    def get_config_from_conn(self):
        conn = self.get_connection(self.apprise_conn_id)
        return json.loads(conn.extra_dejson["config"])

    def set_config_from_conn(self, apprise_obj: apprise.Apprise):
        """Set config from connection to apprise object"""
        config_object = self.get_config_from_conn()
        if isinstance(config_object, list):
            for config in config_object:
                apprise_obj.add(config["path"], tag=config.get("tag", None))
        elif isinstance(config_object, dict):
            apprise_obj.add(config_object["path"], tag=config_object.get("tag", None))
        else:
            raise ValueError("only dict / list[dict] type of values are expected")

    def notify(
        self,
        body: str,
        title: str | None = None,
        notify_type: NotifyType = NotifyType.INFO,
        body_format: NotifyFormat = NotifyFormat.TEXT,
        tag: str | Iterable[str] | None = None,
        attach: str | None = None,
        interpret_escapes: bool | None = None,
        config: AppriseConfig | None = None,
    ):
        r"""
        Send message to plugged-in services

        :param body: Specify the message body
        :param title: Specify the message title. This field is complete optional
        :param notify_type: Specify the message type (default=info). Possible values are "info",
            "success", "failure", and "warning"
        :param body_format: Specify the input message format (default=text). Possible values are "text",
            "html", and "markdown".
        :param tag: Specify one or more tags to filter which services to notify
        :param attach: Specify one or more file attachment locations
        :param interpret_escapes: Enable interpretation of backslash escapes. For example, this would convert
            sequences such as \n and \r to their respected ascii new-line and carriage
        :param config: Specify one or more configuration
        """
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
        pass

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "config": StringField(
                lazy_gettext("config"),
                widget=BS3TextFieldWidget(),
                description='format example - {"path": "service url", "tag": "alerts"} or '
                '[{"path": "service url", "tag": "alerts"},'
                ' {"path": "service url", "tag": "alerts"}]',
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }
