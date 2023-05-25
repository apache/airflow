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
"""This module allows you to transfer mail attachments from a mail server into s3 bucket."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.imap.hooks.imap import ImapHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ImapAttachmentToS3Operator(BaseOperator):
    """
    Transfers a mail attachment from a mail server into s3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ImapAttachmentToS3Operator`

    :param imap_attachment_name: The file name of the mail attachment that you want to transfer.
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket where the file will be downloaded.
    :param s3_key: The destination file name in the s3 bucket for the attachment.
    :param imap_check_regex: If set checks the `imap_attachment_name` for a regular expression.
    :param imap_mail_folder: The folder on the mail server to look for the attachment.
    :param imap_mail_filter: If set other than 'All' only specific mails will be checked.
        See :py:meth:`imaplib.IMAP4.search` for details.
    :param s3_overwrite: If set overwrites the s3 key if already exists.
    :param imap_conn_id: The reference to the connection details of the mail server.
    :param aws_conn_id: AWS connection to use.
    """

    template_fields: Sequence[str] = ("imap_attachment_name", "s3_key", "imap_mail_filter")

    def __init__(
        self,
        *,
        imap_attachment_name: str,
        s3_bucket: str,
        s3_key: str,
        imap_check_regex: bool = False,
        imap_mail_folder: str = "INBOX",
        imap_mail_filter: str = "All",
        s3_overwrite: bool = False,
        imap_conn_id: str = "imap_default",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.imap_attachment_name = imap_attachment_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.imap_check_regex = imap_check_regex
        self.imap_mail_folder = imap_mail_folder
        self.imap_mail_filter = imap_mail_filter
        self.s3_overwrite = s3_overwrite
        self.imap_conn_id = imap_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: Context) -> None:
        """
        This function executes the transfer from the email server (via imap) into s3.

        :param context: The context while executing.
        """
        self.log.info(
            "Transferring mail attachment %s from mail server via imap to s3 key %s...",
            self.imap_attachment_name,
            self.s3_key,
        )

        with ImapHook(imap_conn_id=self.imap_conn_id) as imap_hook:
            imap_mail_attachments = imap_hook.retrieve_mail_attachments(
                name=self.imap_attachment_name,
                check_regex=self.imap_check_regex,
                latest_only=True,
                mail_folder=self.imap_mail_folder,
                mail_filter=self.imap_mail_filter,
            )

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_bytes(
            bytes_data=imap_mail_attachments[0][1],
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=self.s3_overwrite,
        )
