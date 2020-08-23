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

import collections.abc
import logging
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException

log = logging.getLogger(__name__)


def send_email(to: Union[List[str], Iterable[str]], subject: str, html_content: str,
               files=None, dryrun=False, cc=None, bcc=None,
               mime_subtype='mixed', mime_charset='utf-8', **kwargs):
    """
    Send email using backend specified in EMAIL_BACKEND.
    """
    backend = conf.getimport('email', 'EMAIL_BACKEND')
    to_list = get_email_address_list(to)
    to_comma_seperated = ", ".join(to_list)

    return backend(to_comma_seperated, subject, html_content, files=files,
                   dryrun=dryrun, cc=cc, bcc=bcc,
                   mime_subtype=mime_subtype, mime_charset=mime_charset, **kwargs)


def send_email_smtp(
    to: Union[str, Iterable[str]],
    subject: str,
    html_content: str,
    files: Optional[List[str]] = None,
    dryrun: bool = False,
    cc: Optional[Union[str, Iterable[str]]] = None,
    bcc: Optional[Union[str, Iterable[str]]] = None,
    mime_subtype: str = 'mixed',
    mime_charset: str = 'utf-8',
    **kwargs,
):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    smtp_mail_from = conf.get('smtp', 'SMTP_MAIL_FROM')

    msg, recipients = build_mime_message(
        mail_from=smtp_mail_from,
        to=to,
        subject=subject,
        html_content=html_content,
        files=files,
        cc=cc,
        bcc=bcc,
        mime_subtype=mime_subtype,
        mime_charset=mime_charset,
    )

    send_mime_email(e_from=smtp_mail_from, e_to=recipients, mime_msg=msg, dryrun=dryrun)


def build_mime_message(
    mail_from: str,
    to: Union[str, Iterable[str]],
    subject: str,
    html_content: str,
    files: Optional[List[str]] = None,
    cc: Optional[Union[str, Iterable[str]]] = None,
    bcc: Optional[Union[str, Iterable[str]]] = None,
    mime_subtype: str = 'mixed',
    mime_charset: str = 'utf-8',
    custom_headers: Optional[Dict[str, Any]] = None,
) -> Tuple[MIMEMultipart, List[str]]:
    """
    Build a MIME message that can be used to send an email and
    returns full list of recipients.

    :param mail_from: Email address to set as email's from
    :param to: List of email addresses to set as email's to
    :param subject: Email's subject
    :param html_content: Content of email in HTML format
    :param files: List of paths of files to be attached
    :param cc: List of email addresses to set as email's CC
    :param bcc: List of email addresses to set as email's BCC
    :param mime_subtype: Can be used to specify the subtype of the message. Default = mixed
    :param mime_charset: Email's charset. Default = UTF-8.
    :param custom_headers: Additional headers to add to the MIME message.
        No validations are run on these values and they should be able to be encoded.
    :return: Email as MIMEMultipart and list of recipients' addresses.
    """
    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = mail_from
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
        with open(fname, "rb") as file:
            part = MIMEApplication(
                file.read(),
                Name=basename
            )
            part['Content-Disposition'] = f'attachment; filename="{basename}"'
            part['Content-ID'] = f'<{basename}>'
            msg.attach(part)

    if custom_headers:
        for header_key, header_value in custom_headers.items():
            msg[header_key] = header_value

    return msg, recipients


def send_mime_email(e_from: str, e_to: List[str], mime_msg: MIMEMultipart, dryrun: bool = False) -> None:
    """
    Send MIME email.
    """

    smtp_host = conf.get('smtp', 'SMTP_HOST')
    smtp_port = conf.getint('smtp', 'SMTP_PORT')
    smtp_starttls = conf.getboolean('smtp', 'SMTP_STARTTLS')
    smtp_ssl = conf.getboolean('smtp', 'SMTP_SSL')
    smtp_user = None
    smtp_password = None

    try:
        smtp_user = conf.get('smtp', 'SMTP_USER')
        smtp_password = conf.get('smtp', 'SMTP_PASSWORD')
    except AirflowConfigException:
        log.debug("No user/password found for SMTP, so logging in with no authentication.")

    if not dryrun:
        conn = smtplib.SMTP_SSL(smtp_host, smtp_port) if smtp_ssl else smtplib.SMTP(smtp_host, smtp_port)
        if smtp_starttls:
            conn.starttls()
        if smtp_user and smtp_password:
            conn.login(smtp_user, smtp_password)
        log.info("Sent an alert email to %s", e_to)
        conn.sendmail(e_from, e_to, mime_msg.as_string())
        conn.quit()


def get_email_address_list(addresses: Union[str, Iterable[str]]) -> List[str]:
    """
    Get list of email addresses.
    """
    if isinstance(addresses, str):
        return _get_email_list_from_str(addresses)

    elif isinstance(addresses, collections.abc.Iterable):
        if not all(isinstance(item, str) for item in addresses):
            raise TypeError("The items in your iterable must be strings.")
        return list(addresses)

    received_type = type(addresses).__name__
    raise TypeError("Unexpected argument type: Received '{}'.".format(received_type))


def _get_email_list_from_str(addresses: str) -> List[str]:
    delimiters = [",", ";"]
    for delimiter in delimiters:
        if delimiter in addresses:
            return [address.strip() for address in addresses.split(delimiter)]
    return [addresses]
