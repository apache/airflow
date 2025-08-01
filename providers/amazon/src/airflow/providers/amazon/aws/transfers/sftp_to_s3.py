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

from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.version_compat import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SFTPToS3Operator(BaseOperator):
    """
    Transfer files from an SFTP server to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToS3Operator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :param s3_key: The targeted s3 key. This is the specified path for
        uploading the file to S3.
    :param use_temp_file: If True, copies file first to local,
        if False streams file from SFTP to S3.
    :param fail_on_file_not_exist: If True, operator fails when file does not exist,
        if False, operator will not fail and skips transfer. Default is True.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        sftp_path: str,
        sftp_conn_id: str = "ssh_default",
        s3_conn_id: str = "aws_default",
        use_temp_file: bool = True,
        fail_on_file_not_exist: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.use_temp_file = use_temp_file
        self.fail_on_file_not_exist = fail_on_file_not_exist

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    def execute(self, context: Context) -> None:
        self.s3_key = self.get_s3_key(self.s3_key)
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        sftp_client = ssh_hook.get_conn().open_sftp()

        try:
            sftp_client.stat(self.sftp_path)
        except FileNotFoundError:
            if self.fail_on_file_not_exist:
                raise
            self.log.info("File %s not found on SFTP server. Skipping transfer.", self.sftp_path)
            return

        if self.use_temp_file:
            with NamedTemporaryFile("w") as f:
                sftp_client.get(self.sftp_path, f.name)

                s3_hook.load_file(filename=f.name, key=self.s3_key, bucket_name=self.s3_bucket, replace=True)
        else:
            with sftp_client.file(self.sftp_path, mode="rb") as data:
                s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, self.s3_key, Callback=self.log.info)
