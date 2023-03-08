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
"""
This module provides everything to be able to search in mails for a specific attachment
and also to download it.
It uses the imaplib library that is already integrated in python 3.
"""
from __future__ import annotations

import collections.abc
import os
import re
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any, Iterable

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


class SmtpHook(BaseHook):
    """
    TODO: add docstring

    :param smtp_conn_id: The :ref:`imap connection id <howto/connection:imap>`
        that contains the information used to authenticate the client.
    """

    conn_name_attr = "smtp_conn_id"
    default_conn_name = "smtp_default"
    conn_type = "smtp"
    hook_name = "SMTP"

    def __init__(self, smtp_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.smtp_conn_id = smtp_conn_id
        self.smtp_client: smtplib.SMTP_SSL | smtplib.SMTP | None = None

    def __enter__(self) -> SmtpHook:
        return self.get_conn()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.smtp_client.close()

    def get_conn(self) -> SmtpHook:
        """
        Login to the smtp server.

        .. note:: Please call this Hook as context manager via `with`
            to automatically open and close the connection to the smtp server.

        :return: an authorized SmtpHook object.
        """
        if not self.smtp_client:
            try:
                conn = self.get_connection(self.smtp_conn_id)
            except AirflowNotFoundException:
                raise AirflowException("SMTP connection is not found.")
            smtp_user = conn.login
            smtp_password = conn.password
            smtp_starttls = conn.extra_dejson.get("starttls", True)
            smtp_retry_limit = conn.extra_dejson.get("retry_limit", 5)

            for attempt in range(1, smtp_retry_limit + 1):
                try:
                    self.smtp_client = self._build_client(conn=conn)
                except smtplib.SMTPServerDisconnected:
                    if attempt < smtp_retry_limit:
                        continue
                    raise AirflowException("Unable to connect to smtp server")

                self.smtp_client.login(conn.login, conn.password)
                if smtp_starttls:
                    self.smtp_client.starttls()
                if smtp_user and smtp_password:
                    self.smtp_client.login(smtp_user, smtp_password)
                break

        return self

    def _build_client(self, conn: Connection) -> smtplib.SMTP_SSL | smtplib.SMTP:

        SMTP: type[smtplib.SMTP_SSL] | type[smtplib.SMTP]
        if conn.extra_dejson.get("use_ssl", True):
            SMTP = smtplib.SMTP_SSL
        else:
            SMTP = smtplib.SMTP

        smtp_kwargs = {"host": conn.host}
        if conn.port:
            smtp_kwargs["port"] = conn.port
        smtp_kwargs["timeout"] = conn.extra_dejson.get("timeout", 30)

        return SMTP(**smtp_kwargs)

    def send_email_smtp(
        self,
        to: str | Iterable[str],
        from_email: str,
        subject: str,
        html_content: str,
        files: list[str] | None = None,
        dryrun: bool = False,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        mime_subtype: str = "mixed",
        mime_charset: str = "utf-8",
        custom_headers: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """Send an email with html content.

        :param to: Recipient email address or list of addresses.
        :param from_email: Sender email address.
        :param subject: Email subject.
        :param html_content: Email body in HTML format.
        :param files: List of file paths to attach to the email.
        :param dryrun: If True, the email will not be sent, but all other actions will be performed.
        :param cc: Carbon copy recipient email address or list of addresses.
        :param bcc: Blind carbon copy recipient email address or list of addresses.
        :param mime_subtype: MIME subtype of the email.
        :param mime_charset: MIME charset of the email.
        :param custom_headers: Dictionary of custom headers to include in the email.
        :param kwargs: Additional keyword arguments.

        >>> send_email(
                'test@example.com', 'source@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True
            )
        """
        if not self.smtp_client:
            raise Exception("The 'smtp_client' should be initialized before!")

        mime_msg, recipients = self._build_mime_message(
            mail_from=from_email,
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
        if not dryrun:
            self.smtp_client.sendmail(from_addr=from_email, to_addrs=recipients, msg=mime_msg.as_string())

    def _build_mime_message(
        self,
        mail_from: str | None,
        to: str | Iterable[str],
        subject: str,
        html_content: str,
        files: list[str] | None = None,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        mime_subtype: str = "mixed",
        mime_charset: str = "utf-8",
        custom_headers: dict[str, Any] | None = None,
    ) -> tuple[MIMEMultipart, list[str]]:
        """
        Build a MIME message that can be used to send an email and returns a full list of recipients.

        :param mail_from: Email address to set as the email's "From" field.
        :param to: A string or iterable of strings containing email addresses
            to set as the email's "To" field.
        :param subject: The subject of the email.
        :param html_content: The content of the email in HTML format.
        :param files: A list of paths to files to be attached to the email.
        :param cc: A string or iterable of strings containing email addresses
            to set as the email's "CC" field.
        :param bcc: A string or iterable of strings containing email addresses
            to set as the email's "BCC" field.
        :param mime_subtype: The subtype of the MIME message. Default: "mixed".
        :param mime_charset: The charset of the email. Default: "utf-8".
        :param custom_headers: Additional headers to add to the MIME message.
            No validations are run on these values, and they should be able to be encoded.
        :return: A tuple containing the email as a MIMEMultipart object and
            a list of recipient email addresses.
        """
        to = self._get_email_address_list(to)

        msg = MIMEMultipart(mime_subtype)
        msg["Subject"] = subject
        msg["From"] = mail_from
        msg["To"] = ", ".join(to)
        recipients = to
        if cc:
            cc = self._get_email_address_list(cc)
            msg["CC"] = ", ".join(cc)
            recipients += cc

        if bcc:
            # don't add bcc in header
            bcc = self._get_email_address_list(bcc)
            recipients += bcc

        msg["Date"] = formatdate(localtime=True)
        mime_text = MIMEText(html_content, "html", mime_charset)
        msg.attach(mime_text)

        for fname in files or []:
            basename = os.path.basename(fname)
            with open(fname, "rb") as file:
                part = MIMEApplication(file.read(), Name=basename)
                part["Content-Disposition"] = f'attachment; filename="{basename}"'
                part["Content-ID"] = f"<{basename}>"
                msg.attach(part)

        if custom_headers:
            for header_key, header_value in custom_headers.items():
                msg[header_key] = header_value

        return msg, recipients

    def _get_email_address_list(self, addresses: str | Iterable[str]) -> list[str]:
        """
        Returns a list of email addresses from the provided input.

        :param addresses: A string or iterable of strings containing email addresses.
        :return: A list of email addresses.
        :raises TypeError: If the input is not a string or iterable of strings.
        """
        if isinstance(addresses, str):
            return self._get_email_list_from_str(addresses)
        elif isinstance(addresses, collections.abc.Iterable):
            if not all(isinstance(item, str) for item in addresses):
                raise TypeError("The items in your iterable must be strings.")
            return list(addresses)
        else:
            raise TypeError(f"Unexpected argument type: Received '{type(addresses).__name__}'.")

    def _get_email_list_from_str(self, addresses: str) -> list[str]:
        """
        Extract a list of email addresses from a string. The string
        can contain multiple email addresses separated by
        any of the following delimiters: ',' or ';'.

        :param addresses: A string containing one or more email addresses.
        :return: A list of email addresses.
        """
        pattern = r"\s*[,;]\s*"
        return [address for address in re.split(pattern, addresses)]
