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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import mimetypes
import os

import sendgrid
from sendgrid.helpers.mail import Attachment, Content, Email, Mail, \
    Personalization, CustomArg, Category

from airflow.utils.email import get_email_address_list
from airflow.utils.log.logging_mixin import LoggingMixin


def send_email(to, subject, html_content, files=None,
               dryrun=False, cc=None, bcc=None,
               mime_subtype='mixed', **kwargs):
    """
    Send an email with html content using `Sendgrid <https://sendgrid.com/>`__.

    To use this plugin:

    0. Include ``sendgrid`` subpackage as part of your Airflow installation, e.g.,::

       pip install 'apache-airflow[sendgrid]'

    1. Update ``email_backend`` property in `[email]`` section in ``airflow.cfg``, i.e.,::

      [email]
      email_backend = airflow.contrib.utils.sendgrid.send_email

    2. Configure Sendgrid specific environment variables at all Airflow instances:::

      SENDGRID_MAIL_FROM={your-mail-from}
      SENDGRID_API_KEY={your-sendgrid-api-key}.
    """
    mail = Mail()
    from_email = kwargs.get('from_email') or os.environ.get('SENDGRID_MAIL_FROM')
    from_name = kwargs.get('from_name') or os.environ.get('SENDGRID_MAIL_SENDER')
    mail.from_email = Email(from_email, from_name)
    mail.subject = subject

    # Add the recipient list of to emails.
    personalization = Personalization()
    to = get_email_address_list(to)
    for to_address in to:
        personalization.add_to(Email(to_address))
    if cc:
        cc = get_email_address_list(cc)
        for cc_address in cc:
            personalization.add_cc(Email(cc_address))
    if bcc:
        bcc = get_email_address_list(bcc)
        for bcc_address in bcc:
            personalization.add_bcc(Email(bcc_address))

    # Add custom_args to personalization if present
    pers_custom_args = kwargs.get('personalization_custom_args', None)
    if isinstance(pers_custom_args, dict):
        for key in pers_custom_args.keys():
            personalization.add_custom_arg(CustomArg(key, pers_custom_args[key]))

    mail.add_personalization(personalization)
    mail.add_content(Content('text/html', html_content))

    categories = kwargs.get('categories', [])
    for cat in categories:
        mail.add_category(Category(cat))

    # Add email attachment.
    for fname in files or []:
        basename = os.path.basename(fname)
        attachment = Attachment()
        with open(fname, "rb") as f:
            attachment.content = str(base64.b64encode(f.read()), 'utf-8')
            attachment.type = mimetypes.guess_type(basename)[0]
            attachment.filename = basename
            attachment.disposition = "attachment"
            attachment.content_id = '<%s>' % basename
        mail.add_attachment(attachment)
    _post_sendgrid_mail(mail.get())


def _post_sendgrid_mail(mail_data):
    log = LoggingMixin().log
    sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    response = sg.client.mail.send.post(request_body=mail_data)
    # 2xx status code.
    if response.status_code >= 200 and response.status_code < 300:
        log.info('Email with subject %s is successfully sent to recipients: %s' %
                 (mail_data['subject'], mail_data['personalizations']))
    else:
        log.warning('Failed to send out email with subject %s, status code: %s' %
                    (mail_data['subject'], response.status_code))
