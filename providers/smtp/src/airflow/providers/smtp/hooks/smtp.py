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
from collections.abc import Iterable
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.smtp.version_compat import BaseHook

if TYPE_CHECKING:
    try:
        from airflow.sdk import Connection
    except ImportError:
        from airflow.models.connection import Connection  # type: ignore[assignment]


def build_xoauth2_string(username: str, token: str) -> str:
    """Local fallback for older Airflow cores (≤2.11)."""
    return f"user={username}\x01auth=Bearer {token}\x01\x01"


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

    def __init__(self, smtp_conn_id: str = default_conn_name, auth_type: str = "basic") -> None:
        super().__init__()
        self.smtp_conn_id = smtp_conn_id
        self.smtp_connection: Connection | None = None
        self.smtp_client: smtplib.SMTP_SSL | smtplib.SMTP | None = None
        self._auth_type = auth_type
        self._access_token: str | None = None

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

                    # choose auth
                    if self._auth_type == "oauth2":
                        if not self._access_token:
                            self._access_token = self._get_oauth2_token()
                        user_identity = self.smtp_user or self.from_email
                        if user_identity is None:
                            raise AirflowException(
                                "smtp_user or from_email must be set for OAuth2 authentication"
                            )
                        self.smtp_client.auth(
                            "XOAUTH2",
                            lambda _=None: build_xoauth2_string(user_identity, self._access_token),
                        )
                    elif self.smtp_user and self.smtp_password:
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
            ssl_context_string = self.ssl_context
            if ssl_context_string is None or ssl_context_string == "default":
                ssl_context = ssl.create_default_context()
            elif ssl_context_string == "none":
                ssl_context = None
            else:
                raise RuntimeError(
                    f"The connection extra field `ssl_context` must "
                    f"be set to 'default' or 'none' but it is set to '{ssl_context_string}'."
                )
            smtp_kwargs["context"] = ssl_context
        return SMTP(**smtp_kwargs)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField
        from wtforms.validators import NumberRange, any_of

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
            "subject_template": StringField(
                lazy_gettext("Path to the subject template"), widget=BS3TextFieldWidget()
            ),
            "html_content_template": StringField(
                lazy_gettext("Path to the html content template"), widget=BS3TextFieldWidget()
            ),
            "auth_type": StringField(
                lazy_gettext("Auth Type"),
                widget=BS3TextFieldWidget(),
                description="basic  or  oauth2",
                validators=[any_of(["basic", "oauth2"])],
                default="basic",
            ),
            "access_token": StringField(lazy_gettext("Access Token"), widget=BS3TextFieldWidget()),
            "client_id": StringField(lazy_gettext("Client ID"), widget=BS3TextFieldWidget()),
            "client_secret": StringField(lazy_gettext("Client Secret"), widget=BS3TextFieldWidget()),
            "tenant_id": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "scope": StringField(lazy_gettext("Scope"), widget=BS3TextFieldWidget()),
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
        subject: str | None = None,
        html_content: str | None = None,
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
        :param subject: Email subject. If it's None, the hook will check if there is a path to a subject
            file provided in the connection, and raises an exception if not.
        :param html_content: Email body in HTML format. If it's None, the hook will check if there is a path
            to a html content file provided in the connection, and raises an exception if not.
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
        if not subject:
            if self.subject_template is None:
                raise AirflowException(
                    "You should provide `subject` or define `subject_template` in the connection."
                )
            subject = self._read_template(self.subject_template)
        if not html_content:
            if self.html_content_template is None:
                raise AirflowException(
                    "You should provide `html_content` or define `html_content_template` in the connection."
                )
            html_content = self._read_template(self.html_content_template)

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
        if isinstance(addresses, collections.abc.Iterable):
            if not all(isinstance(item, str) for item in addresses):
                raise TypeError("The items in your iterable must be strings.")
            return list(addresses)
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

    def _get_oauth2_token(self) -> str:
        """
        Return a valid OAuth 2.0 access-token.

        If access_token provided in connection extra, then use it.
        Else, try MSAL client-credential flow when client_id & client_secret exist.
        """
        extra = self.conn.extra_dejson

        if token := extra.get("access_token"):
            return token

        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        tenant_id = extra.get("tenant_id", "common")
        scope = extra.get("scope", "https://outlook.office.com/.default")

        if client_id and client_secret:
            from msal import ConfidentialClientApplication

            app = ConfidentialClientApplication(
                client_id=client_id,
                client_credential=client_secret,
                authority=f"https://login.microsoftonline.com/{tenant_id}",
            )
            result = app.acquire_token_for_client(scopes=[scope])
            if "access_token" not in result:
                raise AirflowException(f"Unable to obtain access token: {result.get('error_description')}")
            return result["access_token"]

        raise AirflowException(
            "auth_type='oauth2' but neither 'access_token' nor client credentials supplied in connection extra."
        )

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
        return self.conn.login if self.conn.login else ""

    @property
    def smtp_password(self) -> str:
        return self.conn.password if self.conn.password else ""

    @property
    def smtp_starttls(self) -> bool:
        return not bool(self.conn.extra_dejson.get("disable_tls", False))

    @property
    def host(self) -> str:
        return self.conn.host if self.conn.host else ""

    @property
    def port(self) -> int:
        return cast("int", self.conn.port)

    @property
    def timeout(self) -> int:
        return int(self.conn.extra_dejson.get("timeout", 30))

    @property
    def use_ssl(self) -> bool:
        return not bool(self.conn.extra_dejson.get("disable_ssl", False))

    @property
    def subject_template(self) -> str | None:
        return self.conn.extra_dejson.get("subject_template")

    @property
    def html_content_template(self) -> str | None:
        return self.conn.extra_dejson.get("html_content_template")

    @property
    def ssl_context(self) -> str | None:
        return self.conn.extra_dejson.get("ssl_context")

    @property
    def auth_type(self) -> str:
        return self.conn.extra_dejson.get("auth_type", self._auth_type)

    @property
    def access_token(self) -> str | None:
        if self._access_token:
            return self._access_token
        token = self.conn.extra_dejson.get("access_token")
        if token:
            self._access_token = token
        return self._access_token

    @staticmethod
    def _read_template(template_path: str) -> str:
        """
        Read the content of a template file.

        :param template_path: The path to the template file.
        :return: The content of the template file.
        """
        return Path(template_path).read_text()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }
