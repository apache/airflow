# -*- coding: utf-8 -*-
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

import logging
import os
import boto3

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

from airflow import configuration


def send_email(to, subject, html_content, files=None, dryrun=False, cc=None,
               bcc=None, mime_subtype='mixed', sandbox_mode=False, **kwargs):
    """
    Send an email with html content using Amazon SES.

    To use this plugin:
    0. include ses subpackage as part of your Airflow installation, e.g.,
    pip install apache-airflow[ses]
    1. update [email] backend in airflow.cfg, i.e.,
    [email]
    email_backend = airflow.contrib.utils.ses.send_email
    2. configure SES specific setting in airflow.cfg, e.g.:
    [ses]
    aws_region = eu-west-1
    """
    mail_from = configuration.get('smtp', 'SMTP_MAIL_FROM')
    to = _get_email_address_list(to)

    message = MIMEMultipart(mime_subtype)
    message['Subject'] = subject
    message['From'] = mail_from
    message['To'] = ", ".join(to)
    if cc:
        cc = _get_email_address_list(cc)
        message['Cc'] = ", ".join(cc)
    if bcc:
        bcc = _get_email_address_list(bcc)
        message['Bcc'] = ", ".join(bcc)

    message['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html')
    message.attach(mime_text)

    for file_name in files or []:
        basename = os.path.basename(file_name)
        with open(file_name, "rb") as f:
            message.attach(MIMEApplication(
                f.read(),
                Content_Disposition='attachment; filename="{}"'.format(basename),
                Name=basename
            ))

    if not dryrun:
        _ses_send_raw_email(message)


def _ses_send_raw_email(message):
        aws_region = configuration.get('ses', 'REGION')
        ses = boto3.client('ses', region_name=aws_region)
        data = message.as_string()
        ses.send_raw_email(RawMessage={'Data': data})


def _get_email_address_list(address_string):
    if isinstance(address_string, str):
        if ',' in address_string:
            return address_string.split(',')
        elif ';' in address_string:
            return address_string.split(';')
        else:
            return [address_string]
    else:
        return address_string
