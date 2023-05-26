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
import re
import warnings
from typing import Any, Iterable

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, RemovedInAirflow3Warning
from airflow.models import Connection
from airflow.providers.smtp.hooks.smtp import SmtpHook

log = logging.getLogger(__name__)


class _SmtpHook(SmtpHook):
    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        try:
            connection = super().get_connection(conn_id)
        except Exception:
            connection = Connection()

        extra = connection.extra_dejson

        # try to load some variables from Airflow config to update connection extra
        from_email = conf.get("smtp", "SMTP_MAIL_FROM") or conf.get("email", "from_email", fallback=None)
        if from_email:
            extra["from_email"] = from_email

        smtp_host = conf.get("smtp", "SMTP_HOST", fallback=None)
        if smtp_host:
            connection.host = smtp_host
        smtp_port = conf.getint("smtp", "SMTP_PORT", fallback=None)
        if smtp_port:
            connection.port = smtp_port
        smtp_starttls = conf.getboolean("smtp", "SMTP_STARTTLS", fallback=None)
        if smtp_starttls is not None:
            extra["disable_tls"] = not smtp_starttls
        smtp_ssl = conf.getboolean("smtp", "SMTP_SSL", fallback=None)
        if smtp_ssl is not None:
            extra["disable_ssl"] = not smtp_ssl
        smtp_retry_limit = conf.getint("smtp", "SMTP_RETRY_LIMIT", fallback=None)
        if smtp_retry_limit is not None:
            extra["retry_limit"] = smtp_retry_limit
        smtp_timeout = conf.getint("smtp", "SMTP_TIMEOUT", fallback=None)
        if smtp_timeout is not None:
            extra["timeout"] = smtp_timeout

        # for credentials, we use the connection if it exists, otherwise we use the config
        if connection.login is None or connection.password is None:
            warnings.warn(
                "Fetching SMTP credentials from configuration variables will be deprecated in a future "
                "release. Please set credentials using a connection instead.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            try:
                connection.login = conf.get("smtp", "SMTP_USER")
                connection.password = conf.get("smtp", "SMTP_PASSWORD")
            except AirflowConfigException:
                log.debug("No user/password found for SMTP, so logging in with no authentication.")
        connection.extra = extra
        return connection


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
    backend_conn_id = conn_id or conf.get("email", "EMAIL_CONN_ID")
    if conf.get("email", "EMAIL_BACKEND") == "airflow.utils.email.send_email_smtp":
        warnings.warn(
            "The send_email_smtp function is deprecated."
            " Please use send_email with the SMTP backend instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _SmtpHook(smtp_conn_id=backend_conn_id).send_email_smtp(
            to=to,
            subject=subject,
            html_content=html_content,
            files=files,
            dryrun=dryrun,
            cc=cc,
            bcc=bcc,
            mime_subtype=mime_subtype,
            mime_charset=mime_charset,
            conn_id=backend_conn_id,
            custom_headers=custom_headers,
            **kwargs,
        )

    backend = conf.getimport("email", "EMAIL_BACKEND")
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


def _get_email_list_from_str(addresses: str) -> list[str]:
    """
    Extract a list of email addresses from a string. The string
    can contain multiple email addresses separated by
    any of the following delimiters: ',' or ';'.

    :param addresses: A string containing one or more email addresses.
    :return: A list of email addresses.
    """
    pattern = r"\s*[,;]\s*"
    return [address for address in re.split(pattern, addresses)]
