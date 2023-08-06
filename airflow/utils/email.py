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
from __future__ import annotations

import collections.abc
import logging
import os
import smtplib
import ssl
import warnings
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from typing import Any, Iterable

import re2

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException, RemovedInAirflow3Warning

log = logging.getLogger(__name__)


def send_email(
    to: list[str] | Iterable[str],
    subject: str,
    html_content: str,
    files: list[str] | None = None,
    dryrun: bool = False,
    cc: str | Iterable[str] | None = None,
    bcc: str | Iterable[str] | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    conn_id: str | None = None,
    custom_headers: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """
    Send an email using the backend specified in the *EMAIL_BACKEND* configuration option.

    :param to: A list or iterable of email addresses to send the email to.
    :param subject: The subject of the email.
    :param html_content: The content of the email in HTML format.
    :param files: A list of paths to files to attach to the email.
    :param dryrun: If *True*, the email will not actually be sent. Default: *False*.
    :param cc: A string or iterable of strings containing email addresses to send a copy of the email to.
    :param bcc: A string or iterable of strings containing email addresses to send a
        blind carbon copy of the email to.
    :param mime_subtype: The subtype of the MIME message. Default: "mixed".
    :param mime_charset: The charset of the email. Default: "utf-8".
    :param conn_id: The connection ID to use for the backend. If not provided, the default connection
        specified in the *EMAIL_CONN_ID* configuration option will be used.
    :param custom_headers: A dictionary of additional headers to add to the MIME message.
        No validations are run on these values, and they should be able to be encoded.
    :param kwargs: Additional keyword arguments to pass to the backend.
    """
    backend = conf.getimport("email", "EMAIL_BACKEND")
    backend_conn_id = conn_id or conf.get("email", "EMAIL_CONN_ID")
    from_email = conf.get("email", "from_email", fallback=None)

    to_list = get_email_address_list(to)
    to_comma_separated = ", ".join(to_list)

    return backend(
        to_comma_separated,
        subject,
        html_content,
        files=files,
        dryrun=dryrun,
        cc=cc,
        bcc=bcc,
        mime_subtype=mime_subtype,
        mime_charset=mime_charset,
        conn_id=backend_conn_id,
        from_email=from_email,
        custom_headers=custom_headers,
        **kwargs,
    )


def send_email_smtp(
    to: str | Iterable[str],
    subject: str,
    html_content: str,
    files: list[str] | None = None,
    dryrun: bool = False,
    cc: str | Iterable[str] | None = None,
    bcc: str | Iterable[str] | None = None,
    mime_subtype: str = "mixed",
    mime_charset: str = "utf-8",
    conn_id: str = "smtp_default",
    from_email: str | None = None,
    custom_headers: dict[str, Any] | None = None,
    **kwargs,
) -> None:
    """Send an email with html content.

    :param to: Recipient email address or list of addresses.
    :param subject: Email subject.
    :param html_content: Email body in HTML format.
    :param files: List of file paths to attach to the email.
    :param dryrun: If True, the email will not be sent, but all other actions will be performed.
    :param cc: Carbon copy recipient email address or list of addresses.
    :param bcc: Blind carbon copy recipient email address or list of addresses.
    :param mime_subtype: MIME subtype of the email.
    :param mime_charset: MIME charset of the email.
    :param conn_id: Connection ID of the SMTP server.
    :param from_email: Sender email address.
    :param custom_headers: Dictionary of custom headers to include in the email.
    :param kwargs: Additional keyword arguments.

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    smtp_mail_from = conf.get("smtp", "SMTP_MAIL_FROM")

    if smtp_mail_from is not None:
        mail_from = smtp_mail_from
    else:
        if from_email is None:
            raise Exception(
                "You should set from email - either by smtp/smtp_mail_from config or `from_email` parameter"
            )
        mail_from = from_email

    msg, recipients = build_mime_message(
        mail_from=mail_from,
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

    send_mime_email(e_from=mail_from, e_to=recipients, mime_msg=msg, conn_id=conn_id, dryrun=dryrun)


def build_mime_message(
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
    :param to: A string or iterable of strings containing email addresses to set as the email's "To" field.
    :param subject: The subject of the email.
    :param html_content: The content of the email in HTML format.
    :param files: A list of paths to files to be attached to the email.
    :param cc: A string or iterable of strings containing email addresses to set as the email's "CC" field.
    :param bcc: A string or iterable of strings containing email addresses to set as the email's "BCC" field.
    :param mime_subtype: The subtype of the MIME message. Default: "mixed".
    :param mime_charset: The charset of the email. Default: "utf-8".
    :param custom_headers: Additional headers to add to the MIME message. No validations are run on these
        values, and they should be able to be encoded.
    :return: A tuple containing the email as a MIMEMultipart object and a list of recipient email addresses.
    """
    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = ", ".join(to)
    recipients = to
    if cc:
        cc = get_email_address_list(cc)
        msg["CC"] = ", ".join(cc)
        recipients += cc

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)
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


def send_mime_email(
    e_from: str,
    e_to: str | list[str],
    mime_msg: MIMEMultipart,
    conn_id: str = "smtp_default",
    dryrun: bool = False,
) -> None:
    """
    Send a MIME email.

    :param e_from: The email address of the sender.
    :param e_to: The email address or a list of email addresses of the recipient(s).
    :param mime_msg: The MIME message to send.
    :param conn_id: The ID of the SMTP connection to use.
    :param dryrun: If True, the email will not be sent, but a log message will be generated.
    """
    smtp_host = conf.get_mandatory_value("smtp", "SMTP_HOST")
    smtp_port = conf.getint("smtp", "SMTP_PORT")
    smtp_starttls = conf.getboolean("smtp", "SMTP_STARTTLS")
    smtp_ssl = conf.getboolean("smtp", "SMTP_SSL")
    smtp_retry_limit = conf.getint("smtp", "SMTP_RETRY_LIMIT")
    smtp_timeout = conf.getint("smtp", "SMTP_TIMEOUT")
    smtp_user = None
    smtp_password = None

    if conn_id is not None:
        try:
            from airflow.hooks.base import BaseHook

            airflow_conn = BaseHook.get_connection(conn_id)
            smtp_user = airflow_conn.login
            smtp_password = airflow_conn.password
        except AirflowException:
            pass
    if smtp_user is None or smtp_password is None:
        warnings.warn(
            "Fetching SMTP credentials from configuration variables will be deprecated in a future "
            "release. Please set credentials using a connection instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        try:
            smtp_user = conf.get("smtp", "SMTP_USER")
            smtp_password = conf.get("smtp", "SMTP_PASSWORD")
        except AirflowConfigException:
            log.debug("No user/password found for SMTP, so logging in with no authentication.")

    if not dryrun:
        for attempt in range(1, smtp_retry_limit + 1):
            log.info("Email alerting: attempt %s", str(attempt))
            try:
                smtp_conn = _get_smtp_connection(smtp_host, smtp_port, smtp_timeout, smtp_ssl)
            except smtplib.SMTPServerDisconnected:
                if attempt < smtp_retry_limit:
                    continue
                raise

            if smtp_starttls:
                smtp_conn.starttls()
            if smtp_user and smtp_password:
                smtp_conn.login(smtp_user, smtp_password)
            log.info("Sent an alert email to %s", e_to)
            smtp_conn.sendmail(e_from, e_to, mime_msg.as_string())
            smtp_conn.quit()
            break


def get_email_address_list(addresses: str | Iterable[str]) -> list[str]:
    """
    Returns a list of email addresses from the provided input.

    :param addresses: A string or iterable of strings containing email addresses.
    :return: A list of email addresses.
    :raises TypeError: If the input is not a string or iterable of strings.
    """
    if isinstance(addresses, str):
        return _get_email_list_from_str(addresses)
    elif isinstance(addresses, collections.abc.Iterable):
        if not all(isinstance(item, str) for item in addresses):
            raise TypeError("The items in your iterable must be strings.")
        return list(addresses)
    else:
        raise TypeError(f"Unexpected argument type: Received '{type(addresses).__name__}'.")


def _get_smtp_connection(host: str, port: int, timeout: int, with_ssl: bool) -> smtplib.SMTP:
    """
    Returns an SMTP connection to the specified host and port, with optional SSL encryption.

    :param host: The hostname or IP address of the SMTP server.
    :param port: The port number to connect to on the SMTP server.
    :param timeout: The timeout in seconds for the connection.
    :param with_ssl: Whether to use SSL encryption for the connection.
    :return: An SMTP connection to the specified host and port.
    """
    if not with_ssl:
        return smtplib.SMTP(host=host, port=port, timeout=timeout)
    else:
        ssl_context_string = conf.get("email", "SSL_CONTEXT")
        if ssl_context_string == "default":
            ssl_context = ssl.create_default_context()
        elif ssl_context_string == "none":
            ssl_context = None
        else:
            raise RuntimeError(
                f"The email.ssl_context configuration variable must "
                f"be set to 'default' or 'none' and is '{ssl_context_string}."
            )
        return smtplib.SMTP_SSL(host=host, port=port, timeout=timeout, context=ssl_context)


def _get_email_list_from_str(addresses: str) -> list[str]:
    """
    Extract a list of email addresses from a string.

    The string can contain multiple email addresses separated
    by any of the following delimiters: ',' or ';'.

    :param addresses: A string containing one or more email addresses.
    :return: A list of email addresses.
    """
    pattern = r"\s*[,;]\s*"
    return [address for address in re2.split(pattern, addresses)]
