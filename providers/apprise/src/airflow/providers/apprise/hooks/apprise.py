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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import apprise
from apprise import AppriseConfig, NotifyFormat, NotifyType

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from apprise import AppriseAttachment

    from airflow.providers.common.compat.sdk import Connection


class AppriseHook(BaseHook):
    """
    Use Apprise(https://github.com/caronc/apprise) to interact with notification services.

    The complete list of notification services supported by Apprise can be found at:
    https://github.com/caronc/apprise/wiki#notification-services.

    :param apprise_conn_id: :ref:`Apprise connection id <howto/connection:apprise>`
        that has services configured in the `config` field.
    :param storage_path: Directory used by Apprise persistent storage to cache data such as
        login sessions between notifications. Falls back to ``storage_path`` in the connection
        extra. Persistent storage is not enabled when neither is set.
    :param storage_mode: Apprise persistent storage mode: ``auto``, ``flush`` or ``memory``.
        Falls back to ``storage_mode`` in the connection extra, then to Apprise's default.
        Only used when a storage path is set.
    """

    conn_name_attr = "apprise_conn_id"
    default_conn_name = "apprise_default"
    conn_type = "apprise"
    hook_name = "Apprise"

    def __init__(
        self,
        apprise_conn_id: str = default_conn_name,
        storage_path: str | None = None,
        storage_mode: str | None = None,
    ) -> None:
        super().__init__()
        self.apprise_conn_id = apprise_conn_id
        self.storage_path = storage_path
        self.storage_mode = storage_mode

    def get_config_from_conn(self, conn: Connection):
        config = conn.extra_dejson["config"]
        return json.loads(config) if isinstance(config, str) else config

    def set_config_from_conn(self, conn: Connection, apprise_obj: apprise.Apprise):
        """Set config from connection to apprise object."""
        config_object = self.get_config_from_conn(conn=conn)
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

    def _get_asset(self, conn: Connection | None = None) -> apprise.AppriseAsset | None:
        extra = conn.extra_dejson if conn else {}
        storage_path = self.storage_path or extra.get("storage_path")
        storage_mode = self.storage_mode or extra.get("storage_mode")

        if not storage_path:
            return None

        kwargs: dict[str, Any] = {"storage_path": storage_path}

        if storage_mode:
            valid_modes = sorted(apprise.PERSISTENT_STORE_MODES)
            if storage_mode not in valid_modes:
                raise ValueError(
                    f"Invalid Apprise storage_mode {storage_mode!r}, expected one of {valid_modes}"
                )
            kwargs["storage_mode"] = storage_mode

        return apprise.AppriseAsset(**kwargs)

    def notify(
        self,
        body: str,
        title: str | None = None,
        notify_type: NotifyType = NotifyType.INFO,
        body_format: NotifyFormat = NotifyFormat.TEXT,
        tag: str | Iterable[str] = "all",
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
        title = title or ""

        conn = None if config else self.get_connection(self.apprise_conn_id)
        apprise_obj = apprise.Apprise(asset=self._get_asset(conn))
        if conn:
            self.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)
        elif config:
            apprise_obj.add(config)

        apprise_obj.notify(
            body=body,
            title=title,
            notify_type=notify_type,
            body_format=body_format,
            tag=tag,
            attach=attach,
            interpret_escapes=interpret_escapes,
        )

    async def async_notify(
        self,
        body: str,
        title: str | None = None,
        notify_type: NotifyType = NotifyType.INFO,
        body_format: NotifyFormat = NotifyFormat.TEXT,
        tag: str | Iterable[str] = "all",
        attach: AppriseAttachment | None = None,
        interpret_escapes: bool | None = None,
        config: AppriseConfig | None = None,
    ):
        r"""
        Send message to plugged-in services asynchronously.

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
        title = title or ""

        conn = None if config else await get_async_connection(self.apprise_conn_id)
        apprise_obj = apprise.Apprise(asset=self._get_asset(conn))
        if conn:
            self.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)
        elif config:
            apprise_obj.add(config)

        await apprise_obj.async_notify(
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
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField
        from wtforms.validators import Optional, any_of

        return {
            "config": PasswordField(
                lazy_gettext("config"),
                widget=BS3PasswordFieldWidget(),
                description='format example - {"path": "service url", "tag": "alerts"} or '
                '[{"path": "service url", "tag": "alerts"},'
                ' {"path": "service url", "tag": "alerts"}]',
            ),
            "storage_path": StringField(
                lazy_gettext("Storage path"),
                widget=BS3TextFieldWidget(),
                description="Directory for Apprise persistent storage, e.g. to cache logins between "
                "notifications. Leave empty to disable persistent storage.",
            ),
            "storage_mode": StringField(
                lazy_gettext("Storage mode"),
                widget=BS3TextFieldWidget(),
                validators=[Optional(), any_of(["auto", "flush", "memory"])],
                default="auto",
                description="Apprise persistent storage mode: auto, flush or memory. Defaults to auto.",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }
