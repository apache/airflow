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
"""This module contains AWS SES Hook."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def _get_email_address_list(addresses: str | Iterable[str]) -> list[str]:
    """
    Return a list of email addresses; a string may separate them with ``,`` or ``;``.

    Copied from ``airflow.providers.smtp.utils.emailer.get_email_address_list``.
    """
    if isinstance(addresses, str):
        return re.split(r"\s*[,;]\s*", addresses)
    if isinstance(addresses, Iterable):
        if not all(isinstance(item, str) for item in addresses):
            raise TypeError("The items in your iterable must be strings.")
        return list(addresses)
    raise TypeError(f"Unexpected argument type: Received '{type(addresses).__name__}'.")


def _build_mime_message(
    mail_from: str | None,
    to: str | Iterable[str],
    subject: str,
    html_content: str,
    files: list[str] | None = None,
    cc: str | Iterable[str] | None = None,
    bcc: str | Iterable[str] | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    custom_headers: dict[str, Any] | None = None,
) -> tuple[MIMEMultipart, list[str]]:
    """
    Build the MIME message and return it with the full list of recipients, including CC and BCC.

    Copied from ``airflow.providers.smtp.utils.emailer.build_mime_message``.
    """
    to = _get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg["Subject"] = subject
    if mail_from:
        msg["From"] = mail_from
    msg["To"] = ", ".join(to)
    recipients = to
    if cc:
        cc = _get_email_address_list(cc)
        msg["CC"] = ", ".join(cc)
        recipients += cc

    if bcc:
        # don't add bcc in header
        bcc = _get_email_address_list(bcc)
        recipients += bcc

    msg["Date"] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, "html", mime_charset)
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as file:
            part = MIMEApplication(file.read(), Name=basename)
            part.add_header("Content-Disposition", "attachment", filename=basename)
            part["Content-ID"] = f"<{basename}>"
            msg.attach(part)

    if custom_headers:
        for header_key, header_value in custom_headers.items():
            msg[header_key] = header_value

    return msg, recipients


class SesHook(AwsBaseHook):
    """
    Interact with Amazon Simple Email Service.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("ses") <SES.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "ses"
        super().__init__(*args, **kwargs)

    @staticmethod
    def _build_headers(
        custom_headers: dict[str, Any] | None,
        reply_to: str | None,
        return_path: str | None,
    ) -> dict[str, Any]:
        _custom_headers = custom_headers or {}
        if reply_to:
            _custom_headers["Reply-To"] = reply_to
        if return_path:
            _custom_headers["Return-Path"] = return_path

        return _custom_headers

    def send_email(
        self,
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
    ) -> dict:
        """
        Send email using Amazon Simple Email Service.

        .. seealso::
            - :external+boto3:py:meth:`SES.Client.send_raw_email`

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
        :return: Response from Amazon SES service with unique message identifier.
        """
        custom_headers = self._build_headers(custom_headers, reply_to, return_path)

        message, recipients = _build_mime_message(
            mail_from=mail_from,
            to=to,
            subject=subject,
            html_content=html_content,
            files=files,
            cc=cc,
            bcc=bcc,
            mime_subtype=mime_subtype,
            mime_charset=mime_charset,
            custom_headers=custom_headers,
        )

        return self.conn.send_raw_email(
            Source=mail_from, Destinations=recipients, RawMessage={"Data": message.as_string()}
        )

    async def asend_email(
        self,
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
    ) -> dict:
        """
        Send email using Amazon Simple Email Service (async).

        .. seealso::
            - :external+boto3:py:meth:`SES.Client.send_raw_email`

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
        :return: Response from Amazon SES service with unique message identifier.
        """
        custom_headers = self._build_headers(custom_headers, reply_to, return_path)

        message, recipients = _build_mime_message(
            mail_from=mail_from,
            to=to,
            subject=subject,
            html_content=html_content,
            files=files,
            cc=cc,
            bcc=bcc,
            mime_subtype=mime_subtype,
            mime_charset=mime_charset,
            custom_headers=custom_headers,
        )

        async with await self.get_async_conn() as async_client:
            return await async_client.send_raw_email(
                Source=mail_from, Destinations=recipients, RawMessage={"Data": message.as_string()}
            )
