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

from collections.abc import Iterable
from functools import cached_property

from apprise import AppriseConfig, NotifyFormat, NotifyType

from airflow.providers.apprise.hooks.apprise import AppriseHook
from airflow.providers.apprise.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.notifier import BaseNotifier


class AppriseNotifier(BaseNotifier):
    r"""
    Apprise BaseNotifier.

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
    :param apprise_conn_id: connection that has Apprise configs setup
    """

    template_fields = ("body", "title", "tag", "attach")

    def __init__(
        self,
        *,
        body: str,
        title: str | None = None,
        notify_type: NotifyType = NotifyType.INFO,
        body_format: NotifyFormat = NotifyFormat.TEXT,
        tag: str | Iterable[str] = "all",
        attach: str | None = None,
        interpret_escapes: bool | None = None,
        config: AppriseConfig | None = None,
        apprise_conn_id: str = AppriseHook.default_conn_name,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.apprise_conn_id = apprise_conn_id
        self.body = body
        self.title = title
        self.notify_type = notify_type
        self.body_format = body_format
        self.tag = tag
        self.attach = attach
        self.interpret_escapes = interpret_escapes
        self.config = config

    @cached_property
    def hook(self) -> AppriseHook:
        """Apprise Hook."""
        return AppriseHook(apprise_conn_id=self.apprise_conn_id)

    def notify(self, context):
        """Send a alert to a apprise configured service."""
        self.hook.notify(
            body=self.body,
            title=self.title,
            notify_type=self.notify_type,
            body_format=self.body_format,
            tag=self.tag,
            attach=self.attach,
            interpret_escapes=self.interpret_escapes,
            config=self.config,
        )

    async def async_notify(self, context):
        """Send a alert to a apprise configured service."""
        await self.hook.async_notify(
            body=self.body,
            title=self.title,
            notify_type=self.notify_type,
            body_format=self.body_format,
            tag=self.tag,
            attach=self.attach,
            interpret_escapes=self.interpret_escapes,
            config=self.config,
        )


send_apprise_notification = AppriseNotifier
