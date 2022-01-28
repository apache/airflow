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
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.email import send_email


class EmailOperator(BaseOperator):
    """
    Sends an email.

    :param to: list of emails to send the email to. (templated)
    :param subject: subject line for the email. (templated)
    :param html_content: content of the email, html markup
        is allowed. (templated)
    :param files: file names to attach in email (templated)
    :param cc: list of recipients to be added in CC field
    :param bcc: list of recipients to be added in BCC field
    :param mime_subtype: MIME sub content type
    :param mime_charset: character set parameter added to the Content-Type
        header.
    :param custom_headers: additional headers to add to the MIME message.
    """

    template_fields: Sequence[str] = ('to', 'subject', 'html_content', 'files')
    template_fields_renderers = {"html_content": "html"}
    template_ext: Sequence[str] = ('.html',)
    ui_color = '#e6faf9'

    def __init__(
        self,
        *,
        to: Union[List[str], str],
        subject: str,
        html_content: str,
        files: Optional[List] = None,
        cc: Optional[Union[List[str], str]] = None,
        bcc: Optional[Union[List[str], str]] = None,
        mime_subtype: str = 'mixed',
        mime_charset: str = 'utf-8',
        conn_id: Optional[str] = None,
        custom_headers: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.files = files or []
        self.cc = cc
        self.bcc = bcc
        self.mime_subtype = mime_subtype
        self.mime_charset = mime_charset
        self.conn_id = conn_id
        self.custom_headers = custom_headers

    def execute(self, context: Context):
        send_email(
            self.to,
            self.subject,
            self.html_content,
            files=self.files,
            cc=self.cc,
            bcc=self.bcc,
            mime_subtype=self.mime_subtype,
            mime_charset=self.mime_charset,
            conn_id=self.conn_id,
            custom_headers=self.custom_headers,
        )
