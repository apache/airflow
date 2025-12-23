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
This module provides everything to search mail for a specific attachment and download it.

It uses the imaplib library that is already integrated in python 3.
"""

from __future__ import annotations

import email
import imaplib
import os
import re
import ssl
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class ImapHook(BaseHook):
    """
    This hook connects to a mail server by using the imap protocol.

    .. note:: Please call this Hook as context manager via `with`
        to automatically open and close the connection to the mail server.

    :param imap_conn_id: The :ref:`imap connection id <howto/connection:imap>`
        that contains the information used to authenticate the client.
    """

    conn_name_attr = "imap_conn_id"
    default_conn_name = "imap_default"
    conn_type = "imap"
    hook_name = "IMAP"

    def __init__(self, imap_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.imap_conn_id = imap_conn_id
        self.mail_client: imaplib.IMAP4_SSL | imaplib.IMAP4 | None = None

    def __enter__(self) -> ImapHook:
        return self.get_conn()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mail_client.logout()

    def get_conn(self) -> ImapHook:
        """
        Login to the mail server.

        .. note:: Please call this Hook as context manager via `with`
            to automatically open and close the connection to the mail server.

        :return: an authorized ImapHook object.
        """
        if not self.mail_client:
            conn = self.get_connection(self.imap_conn_id)
            self.mail_client = self._build_client(conn)  # type: ignore[arg-type]
            if conn.login and conn.password:
                self.mail_client.login(conn.login, conn.password)

        return self

    def _build_client(self, conn: Connection) -> imaplib.IMAP4_SSL | imaplib.IMAP4:
        mail_client: imaplib.IMAP4_SSL | imaplib.IMAP4
        host = conn.host or ""
        use_ssl = conn.extra_dejson.get("use_ssl", True)
        if use_ssl:
            from airflow.configuration import conf

            extra_ssl_context = conn.extra_dejson.get("ssl_context", None)
            if extra_ssl_context:
                ssl_context_string = extra_ssl_context
            else:
                ssl_context_string = conf.get("imap", "SSL_CONTEXT", fallback=None)
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
            if conn.port:
                mail_client = imaplib.IMAP4_SSL(host, conn.port, ssl_context=ssl_context)
            else:
                mail_client = imaplib.IMAP4_SSL(host, ssl_context=ssl_context)
        else:
            if conn.port:
                mail_client = imaplib.IMAP4(host, conn.port)
            else:
                mail_client = imaplib.IMAP4(host)

        return mail_client

    def has_mail_attachment(
        self, name: str, *, check_regex: bool = False, mail_folder: str = "INBOX", mail_filter: str = "All"
    ) -> bool:
        """
        Check the mail folder for mails containing attachments with the given name.

        :param name: The name of the attachment that will be searched for.
        :param check_regex: Checks the name for a regular expression.
        :param mail_folder: The mail folder where to look at.
        :param mail_filter: If set other than 'All' only specific mails will be checked.
            See :py:meth:`imaplib.IMAP4.search` for details.
        :returns: True if there is an attachment with the given name and False if not.
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(
            name, check_regex, True, mail_folder, mail_filter
        )
        return bool(mail_attachments)

    def retrieve_mail_attachments(
        self,
        name: str,
        *,
        check_regex: bool = False,
        latest_only: bool = False,
        mail_folder: str = "INBOX",
        mail_filter: str = "All",
        not_found_mode: str = "raise",
    ) -> list[tuple]:
        """
        Retrieve mail's attachments in the mail folder by its name.

        :param name: The name of the attachment that will be downloaded.
        :param check_regex: Checks the name for a regular expression.
        :param latest_only: If set to True it will only retrieve the first matched attachment.
        :param mail_folder: The mail folder where to look at.
        :param mail_filter: If set other than 'All' only specific mails will be checked.
            See :py:meth:`imaplib.IMAP4.search` for details.
        :param not_found_mode: Specify what should happen if no attachment has been found.
            Supported values are 'raise', 'warn' and 'ignore'.
            If it is set to 'raise' it will raise an exception,
            if set to 'warn' it will only print a warning and
            if set to 'ignore' it won't notify you at all.
        :returns: a list of tuple each containing the attachment filename and its payload.
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(
            name, check_regex, latest_only, mail_folder, mail_filter
        )

        if not mail_attachments:
            self._handle_not_found_mode(not_found_mode)

        return mail_attachments

    def download_mail_attachments(
        self,
        name: str,
        local_output_directory: str,
        *,
        check_regex: bool = False,
        latest_only: bool = False,
        mail_folder: str = "INBOX",
        mail_filter: str = "All",
        not_found_mode: str = "raise",
    ) -> None:
        """
        Download mail's attachments in the mail folder by its name to the local directory.

        :param name: The name of the attachment that will be downloaded.
        :param local_output_directory: The output directory on the local machine
            where the files will be downloaded to.
        :param check_regex: Checks the name for a regular expression.
        :param latest_only: If set to True it will only download the first matched attachment.
        :param mail_folder: The mail folder where to look at.
        :param mail_filter: If set other than 'All' only specific mails will be checked.
            See :py:meth:`imaplib.IMAP4.search` for details.
        :param not_found_mode: Specify what should happen if no attachment has been found.
            Supported values are 'raise', 'warn' and 'ignore'.
            If it is set to 'raise' it will raise an exception,
            if set to 'warn' it will only print a warning and
            if set to 'ignore' it won't notify you at all.
        """
        mail_attachments = self._retrieve_mails_attachments_by_name(
            name, check_regex, latest_only, mail_folder, mail_filter
        )

        if not mail_attachments:
            self._handle_not_found_mode(not_found_mode)

        self._create_files(mail_attachments, local_output_directory)

    def _handle_not_found_mode(self, not_found_mode: str) -> None:
        if not_found_mode not in ("raise", "warn", "ignore"):
            self.log.error('Invalid "not_found_mode" %s', not_found_mode)
        elif not_found_mode == "raise":
            raise AirflowException("No mail attachments found!")
        elif not_found_mode == "warn":
            self.log.warning("No mail attachments found!")

    def _retrieve_mails_attachments_by_name(
        self, name: str, check_regex: bool, latest_only: bool, mail_folder: str, mail_filter: str
    ) -> list:
        if not self.mail_client:
            raise RuntimeError("The 'mail_client' should be initialized before!")

        all_matching_attachments = []

        self.mail_client.select(mail_folder)

        for mail_id in self._list_mail_ids_desc(mail_filter):
            response_mail_body = self._fetch_mail_body(mail_id)
            matching_attachments = self._check_mail_body(response_mail_body, name, check_regex, latest_only)

            if matching_attachments:
                all_matching_attachments.extend(matching_attachments)
                if latest_only:
                    break

        self.mail_client.close()

        return all_matching_attachments

    def _list_mail_ids_desc(self, mail_filter: str) -> Iterable[str]:
        if not self.mail_client:
            raise RuntimeError("The 'mail_client' should be initialized before!")
        _, data = self.mail_client.search(None, mail_filter)
        mail_ids = data[0].split()
        return reversed(mail_ids)

    def _fetch_mail_body(self, mail_id: str) -> str:
        if not self.mail_client:
            raise RuntimeError("The 'mail_client' should be initialized before!")
        _, data = self.mail_client.fetch(mail_id, "(RFC822)")
        mail_body = data[0][1]  # type: ignore # The mail body is always in this specific location
        mail_body_str = mail_body.decode("utf-8")  # type: ignore
        return mail_body_str

    def _check_mail_body(
        self, response_mail_body: str, name: str, check_regex: bool, latest_only: bool
    ) -> list[tuple[Any, Any]]:
        mail = Mail(response_mail_body)
        if mail.has_attachments():
            return mail.get_attachments_by_name(name, check_regex, find_first=latest_only)
        return []

    def _create_files(self, mail_attachments: list, local_output_directory: str) -> None:
        for name, payload in mail_attachments:
            if self._is_symlink(name):
                self.log.error("Can not create file because it is a symlink!")
            elif self._is_escaping_current_directory(name):
                self.log.error("Can not create file because it is escaping the current directory!")
            else:
                self._create_file(name, payload, local_output_directory)

    def _is_symlink(self, name: str) -> bool:
        # IMPORTANT NOTE: os.path.islink is not working for windows symlinks
        # See: https://stackoverflow.com/a/11068434
        return os.path.islink(name)

    def _is_escaping_current_directory(self, name: str) -> bool:
        return "../" in name

    def _correct_path(self, name: str, local_output_directory: str) -> str:
        return (
            local_output_directory + name
            if local_output_directory.endswith("/")
            else local_output_directory + "/" + name
        )

    def _create_file(self, name: str, payload: Any, local_output_directory: str) -> None:
        file_path = self._correct_path(name, local_output_directory)

        with open(file_path, "wb") as file:
            file.write(payload)


class Mail(LoggingMixin):
    """
    This class simplifies working with mails returned by the imaplib client.

    :param mail_body: The mail body of a mail received from imaplib client.
    """

    def __init__(self, mail_body: str) -> None:
        super().__init__()
        self.mail = email.message_from_string(mail_body)

    def has_attachments(self) -> bool:
        """
        Check the mail for a attachments.

        :returns: True if it has attachments and False if not.
        """
        return self.mail.get_content_maintype() == "multipart"

    def get_attachments_by_name(
        self, name: str, check_regex: bool, find_first: bool = False
    ) -> list[tuple[Any, Any]]:
        """
        Get all attachments by name for the mail.

        :param name: The name of the attachment to look for.
        :param check_regex: Checks the name for a regular expression.
        :param find_first: If set to True it will only find the first match and then quit.
        :returns: a list of tuples each containing name and payload
            where the attachments name matches the given name.
        """
        attachments = []

        for attachment in self._iterate_attachments():
            found_attachment = (
                attachment.has_matching_name(name) if check_regex else attachment.has_equal_name(name)
            )
            if found_attachment:
                file_name, file_payload = attachment.get_file()
                self.log.info("Found attachment: %s", file_name)
                attachments.append((file_name, file_payload))
                if find_first:
                    break

        return attachments

    def _iterate_attachments(self) -> Iterable[MailPart]:
        for part in self.mail.walk():
            mail_part = MailPart(part)
            if mail_part.is_attachment():
                yield mail_part


class MailPart:
    """
    This class is a wrapper for a Mail object's part and gives it more features.

    :param part: The mail part in a Mail object.
    """

    def __init__(self, part: Any) -> None:
        self.part = part

    def is_attachment(self) -> bool:
        """
        Check if the part is a valid mail attachment.

        :returns: True if it is an attachment and False if not.
        """
        return self.part.get_content_maintype() != "multipart" and self.part.get("Content-Disposition")

    def has_matching_name(self, name: str) -> tuple[Any, Any] | None:
        """
        Check if the given name matches the part's name.

        :param name: The name to look for.
        :returns: True if it matches the name (including regular expression).
        """
        return re.match(name, self.part.get_filename())  # type: ignore

    def has_equal_name(self, name: str) -> bool:
        """
        Check if the given name is equal to the part's name.

        :param name: The name to look for.
        :returns: True if it is equal to the given name.
        """
        return self.part.get_filename() == name

    def get_file(self) -> tuple:
        """
        Get the file including name and payload.

        :returns: the part's name and payload.
        """
        return self.part.get_filename(), self.part.get_payload(decode=True)
