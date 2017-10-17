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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from past.builtins import basestring

import importlib
import mimetypes
import os
import sendgrid
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate
from sendgrid.helpers.mail import Attachment, Content, Email, Mail, Personalization

from airflow import configuration
from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin


def send_email(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed'):
    """
    Send email using backend specified in EMAIL_BACKEND.
    """
    path, attr = configuration.get('email', 'EMAIL_BACKEND').rsplit('.', 1)
    module = importlib.import_module(path)
    backend = getattr(module, attr)
    return backend(to, subject, html_content, files=files, dryrun=dryrun, cc=cc, bcc=bcc, mime_subtype=mime_subtype)


def send_email_sendgrid(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed'):
    """
    Send an email with html content using sendgrid.
    """
    mail = Mail()
    mail.from_email = Email(configuration.get('sendgrid', 'SENDGRID_MAIL_FROM'))
    mail.subject = subject

    # Add the list of to emails.
    to = get_email_address_list(to)
    personalization = Personalization()
    for to_address in to:
      personalization.add_to(Email(to_address))
    mail.add_personalization(personalization)
    mail.add_content(Content('text/html', html_content))

    # Add email attachment.
    for fname in files or []:
        basename = os.path.basename(fname)
        attachment = Attachment()
        with open(fname, "rb") as f:
          attachment.content = base64.b64encode(f.read())
          attachment.type = mimetypes.guess_type(basename)[0]
          attachment.filename = basename
          attachment.disposition = "attachment"
          attachment.content_id = '<%s>' % basename
        mail.add_attachment(attachment)
    _post_sendgrid_mail(mail.get())


def _post_sendgrid_mail(mail_data):
    log = LoggingMixin().log
    sg = sendgrid.SendGridAPIClient(apikey=configuration.get('sendgrid', 'SENDGRID_API_KEY'))
    response = sg.client.mail.send.post(request_body=mail_data)
    # 2xx status code.
    if response.status_code >= 200 and response.status_code < 300:
        log.info('The following email with subject %s is successfully sent to sendgrid.' % subject)
    else:
        log.warning('Failed to send out email with subject %s, status code: %s' % (subject, response.status_code))

def send_email_smtp(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed'):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    SMTP_MAIL_FROM = configuration.get('smtp', 'SMTP_MAIL_FROM')

    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = SMTP_MAIL_FROM
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
    mime_text = MIMEText(html_content, 'html')
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

    send_MIME_email(SMTP_MAIL_FROM, recipients, msg, dryrun)


def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):
    log = LoggingMixin().log

    SMTP_HOST = configuration.get('smtp', 'SMTP_HOST')
    SMTP_PORT = configuration.getint('smtp', 'SMTP_PORT')
    SMTP_STARTTLS = configuration.getboolean('smtp', 'SMTP_STARTTLS')
    SMTP_SSL = configuration.getboolean('smtp', 'SMTP_SSL')
    SMTP_USER = None
    SMTP_PASSWORD = None

    try:
        SMTP_USER = configuration.get('smtp', 'SMTP_USER')
        SMTP_PASSWORD = configuration.get('smtp', 'SMTP_PASSWORD')
    except AirflowConfigException:
        log.debug("No user/password found for SMTP, so logging in with no authentication.")

    if not dryrun:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        log.info("Sent an alert email to %s", e_to)
        s.sendmail(e_from, e_to, mime_msg.as_string())
        s.quit()


def get_email_address_list(address_string):
    if isinstance(address_string, basestring):
        if ',' in address_string:
            address_string = address_string.split(',')
        elif ';' in address_string:
            address_string = address_string.split(';')
        else:
            address_string = [address_string]

    return address_string
