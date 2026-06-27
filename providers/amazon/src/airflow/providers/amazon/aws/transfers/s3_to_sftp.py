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
from airflow.providers.amazon.aws.utils import validate_destination_path
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

if TYPE_CHECKING:
    import paramiko

    from airflow.sdk import Context


class S3ToSFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToSFTPOperator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_path: The sftp remote path. For a single file it must include the
        file path. For multiple files it is the destination directory path and must
        end with ``"/"``.
    :param sftp_remote_host: The remote host of the SFTP server. Overrides host in
        Connection.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
        where the file is downloaded.
    :param s3_key: The targeted s3 key. For a single file it must include the file
        path. For multiple files it is the key prefix (directory) and must end with
        ``"/"``.
    :param s3_filenames: Only used if you want to move multiple files. You can pass
        a list with exact key suffixes present under the s3_key prefix, or a string
        prefix that all filenames must match. Use ``"*"`` to move all objects under
        the s3_key prefix.
    :param sftp_filenames: Only used if you want to move multiple files and name them
        differently at the destination. It can be a list of filenames or a string
        prefix that replaces the s3 prefix.
    :param confirm: specify if the SFTP operation should be confirmed, defaults to True.
        When True, a stat will be performed on the remote file after upload to verify
        the file size matches and confirm successful transfer.
    :param fail_on_file_not_exist: If True, operator fails when a source S3 key does not
        exist. If False, the operator logs a warning and skips the transfer. Default is True.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket", "s3_filenames", "sftp_filenames")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        sftp_path: str,
        sftp_conn_id: str = "ssh_default",
        sftp_remote_host: str = "",
        aws_conn_id: str | None = "aws_default",
        s3_filenames: str | list[str] | None = None,
        sftp_filenames: str | list[str] | None = None,
        confirm: bool = True,
        fail_on_file_not_exist: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sftp_remote_host = sftp_remote_host
        self.aws_conn_id = aws_conn_id
        self.s3_filenames = s3_filenames
        self.sftp_filenames = sftp_filenames
        self.confirm = confirm
        self.fail_on_file_not_exist = fail_on_file_not_exist

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    def _download_from_s3(
        self,
        sftp_client: paramiko.SFTPClient,
        s3_hook: S3Hook,
        s3_key: str,
        sftp_path: str,
    ) -> None:
        validate_destination_path(sftp_path, self.sftp_path, base_name="sftp_path")
        if not s3_hook.check_for_key(s3_key, self.s3_bucket):
            if self.fail_on_file_not_exist:
                raise FileNotFoundError(f"Key {s3_key!r} not found in S3 bucket {self.s3_bucket!r}")
            self.log.info("Key %s not found in S3. Skipping transfer.", s3_key)
            return
        with NamedTemporaryFile("w") as f:
            s3_hook.get_conn().download_file(self.s3_bucket, s3_key, f.name)
            sftp_client.put(f.name, sftp_path, confirm=self.confirm)

    def execute(self, context: Context) -> None:
        self.s3_key = self.get_s3_key(self.s3_key)

        # SSHHook will handle a None/"" sftp_remote_host
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id, remote_host=self.sftp_remote_host)
        s3_hook = S3Hook(self.aws_conn_id)
        sftp_client = ssh_hook.get_conn().open_sftp()

        if self.s3_filenames:
            if isinstance(self.s3_filenames, str):
                self.log.info("Getting files in s3://%s/%s", self.s3_bucket, self.s3_key)
                all_keys = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key) or []
                filenames = [k[len(self.s3_key) :] for k in all_keys]
                if self.s3_filenames == "*":
                    files = filenames
                else:
                    s3_prefix: str = self.s3_filenames
                    files = [f for f in filenames if s3_prefix in f]

                for file in files:
                    self.log.info("Moving file %s", file)
                    if self.sftp_filenames and isinstance(self.sftp_filenames, str):
                        sftp_filename = file.replace(self.s3_filenames, self.sftp_filenames)
                    else:
                        sftp_filename = file
                    self._download_from_s3(
                        sftp_client,
                        s3_hook,
                        self.s3_key + file,
                        self.sftp_path + sftp_filename,
                    )
            else:
                if self.sftp_filenames:
                    for s3_file, sftp_file in zip(self.s3_filenames, self.sftp_filenames):
                        self._download_from_s3(
                            sftp_client,
                            s3_hook,
                            self.s3_key + s3_file,
                            self.sftp_path + sftp_file,
                        )
                else:
                    for s3_file in self.s3_filenames:
                        self._download_from_s3(
                            sftp_client,
                            s3_hook,
                            self.s3_key + s3_file,
                            self.sftp_path + s3_file,
                        )
        else:
            self._download_from_s3(sftp_client, s3_hook, self.s3_key, self.sftp_path)
