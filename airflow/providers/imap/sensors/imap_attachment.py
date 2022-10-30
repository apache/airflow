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
"""This module allows you to poke for attachments on a mail server."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.imap.hooks.imap import ImapHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ImapAttachmentSensor(BaseSensorOperator):
    """
    Waits for a specific attachment on a mail server.

    :param attachment_name: The name of the attachment that will be checked.
    :param check_regex: If set to True the attachment's name will be parsed as regular expression.
        Through this you can get a broader set of attachments
        that it will look for than just only the equality of the attachment name.
    :param mail_folder: The mail folder in where to search for the attachment.
    :param mail_filter: If set other than 'All' only specific mails will be checked.
        See :py:meth:`imaplib.IMAP4.search` for details.
    :param imap_conn_id: The :ref:`imap connection id <howto/connection:imap>` to run the sensor against.
    """

    template_fields: Sequence[str] = ("attachment_name", "mail_filter")

    def __init__(
        self,
        *,
        attachment_name,
        check_regex=False,
        mail_folder="INBOX",
        mail_filter="All",
        conn_id="imap_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.attachment_name = attachment_name
        self.check_regex = check_regex
        self.mail_folder = mail_folder
        self.mail_filter = mail_filter
        self.conn_id = conn_id

    def poke(self, context: Context) -> bool:
        """
        Pokes for a mail attachment on the mail server.

        :param context: The context that is being provided when poking.
        :return: True if attachment with the given name is present and False if not.
        """
        self.log.info("Poking for %s", self.attachment_name)

        with ImapHook(imap_conn_id=self.conn_id) as imap_hook:
            return imap_hook.has_mail_attachment(
                name=self.attachment_name,
                check_regex=self.check_regex,
                mail_folder=self.mail_folder,
                mail_filter=self.mail_filter,
            )
