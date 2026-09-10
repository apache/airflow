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

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.common.compat.sdk import conf
from airflow.providers.microsoft.azure.hooks.msgraph import MSGraphMailHook
from airflow.providers.microsoft.azure.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.providers.common.compat.sdk import Context


class MSGraphNotifier(BaseNotifier):
    """
    Send an email from an Office 365 mailbox through the Microsoft Graph ``sendMail`` endpoint.

    .. code-block:: python

        EmptyOperator(
            task_id="task",
            on_failure_callback=MSGraphNotifier(
                from_email="airflow@example.com",
                to="team@example.com",
                subject="Task {{ ti.task_id }} failed",
                html_content="Dag {{ ti.dag_id }} failed on {{ ds }}",
            ),
        )

    :param to: Recipient email address or list of addresses.
    :param subject: Email subject.
    :param html_content: Email body in HTML format.
    :param from_email: The mailbox the message is sent from. Falls back to the ``[email] from_email``
        configuration option.
    :param files: List of file paths to attach to the email.
    :param cc: Carbon copy recipient email address or list of addresses.
    :param bcc: Blind carbon copy recipient email address or list of addresses.
    :param custom_headers: Custom internet message headers, whose names have to start with "x-".
    :param conn_id: The :ref:`Microsoft Graph API connection id <howto/connection:msgraph>`.
    :param save_to_sent_items: Whether the message is saved in the mailbox's Sent Items folder.
    """

    template_fields = (
        "from_email",
        "to",
        "subject",
        "html_content",
        "files",
        "cc",
        "bcc",
        "custom_headers",
    )

    def __init__(
        self,
        to: str | Iterable[str],
        subject: str,
        html_content: str,
        from_email: str | None = None,
        files: list[str] | None = None,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        custom_headers: dict[str, Any] | None = None,
        conn_id: str = MSGraphMailHook.default_conn_name,
        save_to_sent_items: bool = True,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.from_email = from_email or conf.get("email", "from_email", fallback=None)
        self.files = files
        self.cc = cc
        self.bcc = bcc
        self.custom_headers = custom_headers
        self.conn_id = conn_id
        self.save_to_sent_items = save_to_sent_items

    @cached_property
    def hook(self) -> MSGraphMailHook:
        """Microsoft Graph mail hook."""
        return MSGraphMailHook(conn_id=self.conn_id)

    def notify(self, context: Context) -> None:
        """Send an email through Microsoft Graph."""
        self.hook.send_email(**self._build_email_arguments())

    async def async_notify(self, context: Context) -> None:
        """Send an email through Microsoft Graph (async)."""
        await self.hook.asend_email(**self._build_email_arguments())

    def _build_email_arguments(self) -> dict[str, Any]:
        return {
            "from_email": self.from_email,
            "to": self.to,
            "subject": self.subject,
            "html_content": self.html_content,
            "files": self.files,
            "cc": self.cc,
            "bcc": self.bcc,
            "custom_headers": self.custom_headers,
            "save_to_sent_items": self.save_to_sent_items,
        }


send_msgraph_notification = MSGraphNotifier
