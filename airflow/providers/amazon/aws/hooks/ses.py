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

from typing import Any, Iterable

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.email import build_mime_message


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
        :param mime_subtype: Can be used to specify the sub-type of the message. Default = mixed
        :param mime_charset: Email's charset. Default = UTF-8.
        :param return_path: The email address to which replies will be sent. By default, replies
            are sent to the original sender's email address.
        :param reply_to: The email address to which message bounces and complaints should be sent.
            "Return-Path" is sometimes called "envelope from", "envelope sender", or "MAIL FROM".
        :param custom_headers: Additional headers to add to the MIME message.
            No validations are run on these values and they should be able to be encoded.
        :return: Response from Amazon SES service with unique message identifier.
        """
        ses_client = self.get_conn()

        custom_headers = custom_headers or {}
        if reply_to:
            custom_headers["Reply-To"] = reply_to
        if return_path:
            custom_headers["Return-Path"] = return_path

        message, recipients = build_mime_message(
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

        return ses_client.send_raw_email(
            Source=mail_from, Destinations=recipients, RawMessage={"Data": message.as_string()}
        )
