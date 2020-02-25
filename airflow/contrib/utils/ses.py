# -*- coding: utf-8 -*-
# A near-clone of airflow.utils.email that uses the boto3 client to send email directly via SES instead of over the SMTP interface 
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from past.builtins import basestring

import os
import uuid

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate

import boto3
import six

from airflow import configuration
from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin


def send_ses_email(to, subject, html_content, files=None,
                   dryrun=False, cc=None, bcc=None,
                   mime_subtype='mixed', mime_charset='utf-8',
                   **kwargs):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    SES_MAIL_FROM = configuration.conf.get('ses', 'SES_MAIL_FROM')
    if not six.PY3:
        # Works around futures newstr being incompatable with urllib
        # https://github.com/tumblr/pytumblr/pull/94/files/d31889bb3b132c24b1ebf72e939d66f97e3974b2
        SES_MAIL_FROM = bytes(SES_MAIL_FROM)

    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)

    msg['Subject'] = subject
    msg['From'] = SES_MAIL_FROM
    msg['To'] = ", ".join(to)
    recipients = to
    if cc:
        cc = get_email_address_list(cc)
        msg['CC'] = ", ".join(cc)
        recipients = recipients + cc

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)
        recipients = recipients + bcc

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html', mime_charset)
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            part['Content-ID'] = '<%s>' % basename
            msg.attach(part)

    send_MIME_email(SES_MAIL_FROM, recipients, msg, dryrun)


def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):
    log = LoggingMixin().log

    if not dryrun:
        SES_REGION = configuration.conf.get('ses', 'SES_REGION')
        client = boto3.client('ses', region_name=SES_REGION)
        log.info("Sent an alert email to %s", e_to)
        client.send_raw_email(Source=e_from,
                              Destinations=e_to,
                              RawMessage={
                                  'Data': mime_msg.as_string()
                              })


def get_email_address_list(address_string):
    if isinstance(address_string, basestring):
        if ',' in address_string:
            address_string = [address.strip() for address in address_string.split(',')]
        elif ';' in address_string:
            address_string = [address.strip() for address in address_string.split(';')]
        else:
            address_string = [address_string]

    return address_string
