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
Search in emails for a specific attachment and also to download it.

It uses the smtplib library that is already integrated in python 3.
"""

from __future__ import annotations

import collections.abc
import os
import re
import smtplib
import ssl
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import TYPE_CHECKING, Any, Iterable

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class SmtpHook(BaseHook):
    """
    This hook connects to a mail server by using the smtp protocol.

    .. note:: Please call this Hook as context manager via `with`
        to automatically open and close the connection to the mail server.

    :param smtp_conn_id: The :ref:`smtp connection id <howto/connection:smtp>`
        that contains the information used to authenticate the client.
    """

    conn_name_attr = "smtp_conn_id"
    default_conn_name = "smtp_default"
    conn_type = "smtp"
    hook_name = "SMTP"

    def __init__(self, smtp_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.smtp_conn_id = smtp_conn_id
        self.smtp_connection: Connection | None = None
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
                self.smtp_connection = self.get_connection(self.smtp_conn_id)
            except AirflowNotFoundException:
                raise AirflowException("SMTP connection is not found.")

            for attempt in range(1, self.smtp_retry_limit + 1):
                try:
                    self.smtp_client = self._build_client()
                except smtplib.SMTPServerDisconnected:
                    if attempt == self.smtp_retry_limit:
                        raise AirflowException("Unable to connect to smtp server")
                else:
                    if self.smtp_starttls:
                        self.smtp_client.starttls()
                    if self.smtp_user and self.smtp_password:
                        self.smtp_client.login(self.smtp_user, self.smtp_password)
                    break

        return self

    def _build_client(self) -> smtplib.SMTP_SSL | smtplib.SMTP:
        SMTP: type[smtplib.SMTP_SSL] | type[smtplib.SMTP]
        if self.use_ssl:
            SMTP = smtplib.SMTP_SSL
        else:
            SMTP = smtplib.SMTP

        smtp_kwargs: dict[str, Any] = {"host": self.host}
        if self.port:
            smtp_kwargs["port"] = self.port
        smtp_kwargs["timeout"] = self.timeout

        if self.use_ssl:
            from airflow.configuration import conf

            extra_ssl_context = self.conn.extra_dejson.get("ssl_context", None)
            if extra_ssl_context:
                ssl_context_string = extra_ssl_context
            else:
                ssl_context_string = conf.get("smtp_provider", "SSL_CONTEXT", fallback=None)
            if ssl_context_string is None:
                ssl_context_string = conf.get("email", "SSL_CONTEXT", fallback=None)
            if ssl_context_string is None:
                ssl_context_string = "default"
            if ssl_context_string == "default":
                ssl_context = ssl.create_default_context()
            elif ssl_context_string == "none":
                ssl_context = None
            else:
                raise RuntimeError(
                    f"The email.ssl_context configuration variable must "
                    f"be set to 'default' or 'none' and is '{ssl_context_string}'."
                )
            smtp_kwargs["context"] = ssl_context
        return SMTP(**smtp_kwargs)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField
        from wtforms.validators import NumberRange

        return {
            "from_email": StringField(lazy_gettext("From email"), widget=BS3TextFieldWidget()),
            "timeout": IntegerField(
                lazy_gettext("Connection timeout"),
                validators=[NumberRange(min=0)],
                widget=BS3TextFieldWidget(),
                default=30,
            ),
            "retry_limit": IntegerField(
                lazy_gettext("Number of Retries"),
                validators=[NumberRange(min=0)],
                widget=BS3TextFieldWidget(),
                default=5,
            ),
            "disable_tls": BooleanField(lazy_gettext("Disable TLS"), default=False),
            "disable_ssl": BooleanField(lazy_gettext("Disable SSL"), default=False),
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test SMTP connectivity from UI."""
        try:
            smtp_client = self.get_conn().smtp_client
            if smtp_client:
                status = smtp_client.noop()[0]
                if status == 250:
                    return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
        return False, "Failed to establish connection"

    def send_email_smtp(
        self,
        *,
        to: str | Iterable[str],
        subject: str,
        html_content: str,
        from_email: str | None = None,
        files: list[str] | None = None,
        dryrun: bool = False,
        cc: str | Iterable[str] | None = None,
        bcc: str | Iterable[str] | None = None,
        mime_subtype: str = "mixed",
        mime_charset: str = "utf-8",
        custom_headers: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """
        Send an email with html content.

        :param to: Recipient email address or list of addresses.
        :param subject: Email subject.
        :param html_content: Email body in HTML format.
        :param from_email: Sender email address. If it's None, the hook will check if there is an email
            provided in the connection, and raises an exception if not.
        :param files: List of file paths to attach to the email.
        :param dryrun: If True, the email will not be sent, but all other actions will be performed.
        :param cc: Carbon copy recipient email address or list of addresses.
        :param bcc: Blind carbon copy recipient email address or list of addresses.
        :param mime_subtype: MIME subtype of the email.
        :param mime_charset: MIME charset of the email.
        :param custom_headers: Dictionary of custom headers to include in the email.
        :param kwargs: Additional keyword arguments.

        >>> send_email_smtp(
                'test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True
            )
        """
        if not self.smtp_client:
            raise AirflowException("The 'smtp_client' should be initialized before!")
        from_email = from_email or self.from_email
        if not from_email:
            raise AirflowException("You should provide `from_email` or define it in the connection.")

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
            for attempt in range(1, self.smtp_retry_limit + 1):
                try:
                    self.smtp_client.sendmail(
                        from_addr=from_email, to_addrs=recipients, msg=mime_msg.as_string()
                    )
                except smtplib.SMTPServerDisconnected as e:
                    if attempt == self.smtp_retry_limit:
                        raise e
                else:
                    break

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
        if mail_from:
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
        Return a list of email addresses from the provided input.

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
        Extract a list of email addresses from a string.

        The string can contain multiple email addresses separated by
        any of the following delimiters: ',' or ';'.

        :param addresses: A string containing one or more email addresses.
        :return: A list of email addresses.
        """
        pattern = r"\s*[,;]\s*"
        return re.split(pattern, addresses)

    @property
    def conn(self) -> Connection:
        if not self.smtp_connection:
            raise AirflowException("The smtp connection should be loaded before!")
        return self.smtp_connection

    @property
    def smtp_retry_limit(self) -> int:
        return int(self.conn.extra_dejson.get("retry_limit", 5))

    @property
    def from_email(self) -> str | None:
        return self.conn.extra_dejson.get("from_email")

    @property
    def smtp_user(self) -> str:
        return self.conn.login

    @property
    def smtp_password(self) -> str:
        return self.conn.password

    @property
    def smtp_starttls(self) -> bool:
        return not bool(self.conn.extra_dejson.get("disable_tls", False))

    @property
    def host(self) -> str:
        return self.conn.host

    @property
    def port(self) -> int:
        return self.conn.port

    @property
    def timeout(self) -> int:
        return int(self.conn.extra_dejson.get("timeout", 30))

    @property
    def use_ssl(self) -> bool:
        return not bool(self.conn.extra_dejson.get("disable_ssl", False))

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }
