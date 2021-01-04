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
"""This module contains AWS SES Operator"""
from typing import Any, Dict, Iterable, List, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.ses import SESHook
from airflow.utils.decorators import apply_defaults


class SESSendEmailOperator(BaseOperator):
    """
    Send email using Amazon Simple Email Service

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
        "Return-Path" is sometimes called "envelope from," "envelope sender," or "MAIL FROM."
    :param custom_headers: Additional headers to add to the MIME message.
        No validations are run on these values and they should be able to be encoded.
    :return: Response from Amazon SES service with unique message identifier.
    """

    template_fields = ("mail_from", "to", "subject", "html_content")
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self,
        *,
        mail_from: str,
        to: Union[str, Iterable[str]],
        subject: str,
        html_content: str,
        files: Optional[List[str]] = None,
        cc: Optional[Union[str, Iterable[str]]] = None,
        bcc: Optional[Union[str, Iterable[str]]] = None,
        mime_subtype: str = 'mixed',
        mime_charset: str = 'utf-8',
        reply_to: Optional[str] = None,
        return_path: Optional[str] = None,
        custom_headers: Optional[Dict[str, Any]] = None,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ) -> None:
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
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        hook = SESHook(aws_conn_id=self.aws_conn_id)
        return hook.send_email(
            mail_from = self.mail_from,
            to = self.to,
            subject = self.subject,
            html_content = self.html_content,
            files = self.files,
            cc = self.cc,
            bcc = self.bcc,
            mime_subtype = self.mime_subtype,
            mime_charset = self.mime_charset,
            reply_to = self.reply_to,
            return_path = self.return_path,
            custom_headers = self.custom_headers,
        )
