# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.models import BaseOperator
from airflow.utils.email import send_email
from airflow.utils.decorators import apply_defaults


class EmailOperator(BaseOperator):
    """
    Sends an email.

    :param to: list of emails to send the email to
    :type to: list or string (comma or semicolon delimited)
    :param subject: subject line for the email (templated)
    :type subject: string
    :param html_content: content of the email (templated), html markup
        is allowed
    :type html_content: string
    :param files: file names to attach in email
    :type files: list
    :param cc: list of recipients to be added in CC field
    :type cc: list or string (comma or semicolon delimited)
    :param bcc: list of recipients to be added in BCC field
    :type bcc: list or string (comma or semicolon delimited)
    """

    template_fields = ('to', 'subject', 'html_content')
    template_ext = ('.html',)
    ui_color = '#e6faf9'

    @apply_defaults
    def __init__(
            self,
            to,
            subject,
            html_content,
            files=None,
            cc=None,
            bcc=None,
            mime_subtype='mixed',
            *args, **kwargs):
        super(EmailOperator, self).__init__(*args, **kwargs)
        self.to = to
        self.subject = subject
        self.html_content = html_content
        self.files = files or []
        self.cc = cc
        self.bcc = bcc
        self.mime_subtype = mime_subtype

    def execute(self, context):
        send_email(self.to, self.subject, self.html_content, files=self.files, cc=self.cc, bcc=self.bcc, mime_subtype=self.mime_subtype)
