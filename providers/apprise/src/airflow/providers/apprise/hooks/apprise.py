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
from apprise import AppriseAsset, AppriseConfig, NotifyFormat, NotifyType

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
    :param storage_path: Optional filesystem path to enable Apprise persistent storage.
        When set, login sessions and tokens are cached to disk, avoiding repeated
        authentication (e.g., Matrix login rate limits). If not provided, persistent
        storage is disabled and behavior is unchanged from previous versions.
    :param storage_mode: Persistent storage mode. Only used when ``storage_path`` is set.
        Valid values: ``"auto"`` (write on demand, default), ``"flush"`` (write always),
        ``"memory"`` (disable disk writes). Maps to ``apprise.PersistentStoreMode``.
    """

    conn_name_attr = "apprise_conn_id"
    default_conn_name = "apprise_default"
    conn_type = "apprise"
    hook_name = "Apprise"

    def __init__(
        self,
        apprise_conn_id: str = default_conn_name,
        storage_path: str | None = None,
        storage_mode: str = "auto",
    ) -> None:
        super().__init__()
        self.apprise_conn_id = apprise_conn_id
        self.storage_path = storage_path
        self.storage_mode = storage_mode

    def _build_apprise_asset(self) -> AppriseAsset | None:
        """Build an AppriseAsset with persistent storage if storage_path is configured."""
        if not self.storage_path:
            return None

        mode_map = {
            "auto": apprise.PersistentStoreMode.AUTO,
            "flush": apprise.PersistentStoreMode.FLUSH,
            "memory": apprise.PersistentStoreMode.MEMORY,
        }
        mode = mode_map.get(self.storage_mode.lower(), apprise.PersistentStoreMode.AUTO)
        return AppriseAsset(storage_path=self.storage_path, storage_mode=mode)

    def _create_apprise_obj(self) -> apprise.Apprise:
        """Create an Apprise instance, optionally with persistent storage."""
        asset = self._build_apprise_asset()
        if asset:
            return apprise.Apprise(asset=asset)
        return apprise.Apprise()

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
        apprise_obj = self._create_apprise_obj()
        if config:
            apprise_obj.add(config)
        else:
            conn = self.get_connection(self.apprise_conn_id)
            self.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)
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
        apprise_obj = self._create_apprise_obj()
        if config:
            apprise_obj.add(config)
        else:
            conn = await get_async_connection(self.apprise_conn_id)
            self.set_config_from_conn(conn=conn, apprise_obj=apprise_obj)
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
