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

from collections.abc import Iterable, Sequence
from functools import cached_property
from typing import Any

from airflow.providers.amazon.aws.hooks.ses import SesHook
from airflow.providers.amazon.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.utils.helpers import prune_dict


class SesNotifier(BaseNotifier):
    """
    Amazon Simple Email Service (SES) Notifier.

    :param mail_from: Email address to set as email's from
    :param to: List of email addresses to set as email's to
    :param subject: Email's subject
    :param html_content: Content of email in HTML format
    :param files: List of paths of files to be attached
    :param cc: List of email addresses to set as email's CC
    :param bcc: List of email addresses to set as email's BCC
    :param mime_subtype: Can be used to specify the subtype of the message. Default = mixed
    :param mime_charset: Email's charset. Default = UTF-8.
    :param return_path: The email address to which replies will be sent. By default, replies
        are sent to the original sender's email address.
    :param reply_to: The email address to which message bounces and complaints should be sent.
        "Return-Path" is sometimes called "envelope from", "envelope sender", or "MAIL FROM".
    :param custom_headers: Additional headers to add to the MIME message.
        No validations are run on these values, and they should be able to be encoded.
    """

    template_fields: Sequence[str] = (
        "aws_conn_id",
        "region_name",
        "mail_from",
        "to",
        "subject",
        "html_content",
        "files",
        "cc",
        "bcc",
        "mime_subtype",
        "mime_charset",
        "reply_to",
        "return_path",
        "custom_headers",
    )

    def __init__(
        self,
        *,
        aws_conn_id: str | None = SesHook.default_conn_name,
        region_name: str | None = None,
        mail_from: str,
        to: str | Iterable[str],
        subject: str,
        html_content: str,
        files: list[str] | None = None,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        mime_subtype: str = "mixed",
        mime_charset: str = "utf-8",
        reply_to: str | None = None,
        return_path: str | None = None,
        custom_headers: dict[str, Any] | None = None,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

        self.mail_from = mail_from
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.files = files
        self.cc = cc
        self.bcc = bcc
        self.mime_subtype = mime_subtype
        self.mime_charset = mime_charset
        self.reply_to = reply_to
        self.return_path = return_path
        self.custom_headers = custom_headers

    def _build_send_kwargs(self):
        return prune_dict(
            {
                "mail_from": self.mail_from,
                "to": self.to,
                "subject": self.subject,
                "html_content": self.html_content,
                "files": self.files,
                "cc": self.cc,
                "bcc": self.bcc,
                "mime_subtype": self.mime_subtype,
                "mime_charset": self.mime_charset,
                "reply_to": self.reply_to,
                "return_path": self.return_path,
                "custom_headers": self.custom_headers,
            }
        )

    @cached_property
    def hook(self) -> SesHook:
        """Amazon Simple Email Service (SES) Hook (cached)."""
        return SesHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def notify(self, context):
        """Send email using Amazon Simple Email Service (SES)."""
        self.hook.send_email(**self._build_send_kwargs())

    async def async_notify(self, context):
        """Send email using Amazon Simple Email Service (SES) (async)."""
        await self.hook.asend_email(**self._build_send_kwargs())


send_ses_notification = SesNotifier
