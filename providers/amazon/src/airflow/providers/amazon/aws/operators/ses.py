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
"""Send email using Amazon Simple Email Service (SES)."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.ses import SesHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SesEmailOperator(AwsBaseOperator[SesHook]):
    """
    Send an email using Amazon Simple Email Service (SES).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SesEmailOperator`

    :param mail_from: Email address to set as email's from (templated)
    :param to: List of email addresses to set as email's to (templated)
    :param subject: Email's subject (templated)
    :param html_content: Content of email in HTML format (templated)
    :param files: List of paths of files to be attached
    :param cc: List of email addresses to set as email's CC (templated)
    :param bcc: List of email addresses to set as email's BCC (templated)
    :param mime_subtype: Can be used to specify the subtype of the message. Default = mixed
    :param mime_charset: Email's charset. Default = UTF-8
    :param reply_to: The email address to which replies will be sent
    :param return_path: The email address to which message bounces and complaints should be sent
    :param custom_headers: Additional headers to add to the MIME message
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = SesHook
    template_fields: Sequence[str] = aws_template_fields(
        "mail_from",
        "to",
        "subject",
        "html_content",
        "cc",
        "bcc",
        "mime_subtype",
        "mime_charset",
        "reply_to",
        "return_path",
        "custom_headers",
    )
    template_fields_renderers = {
        "custom_headers": "json",
    }

    def __init__(
        self,
        *,
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
        super().__init__(**kwargs)
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

    def execute(self, context: Context) -> dict:
        """
        Send email using Amazon SES.

        :param context: The task context
        :return: Response from Amazon SES service with unique message identifier
        """
        self.log.info(
            "Sending email via Amazon SES from %s to %s with subject: %s",
            self.mail_from,
            self.to,
            self.subject,
        )

        response = self.hook.send_email(
            mail_from=self.mail_from,
            to=self.to,
            subject=self.subject,
            html_content=self.html_content,
            files=self.files,
            cc=self.cc,
            bcc=self.bcc,
            mime_subtype=self.mime_subtype,
            mime_charset=self.mime_charset,
            reply_to=self.reply_to,
            return_path=self.return_path,
            custom_headers=self.custom_headers,
        )

        self.log.info("Email sent successfully. Message ID: %s", response.get("MessageId"))
        return response
