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

import warnings
from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

if TYPE_CHECKING:
    import paramiko

    from airflow.sdk import Context


class SFTPToS3Operator(BaseOperator):
    """
    Transfer files from an SFTP server to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToS3Operator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_remote_host: The remote host of the SFTP server. Overrides host in
        Connection.
    :param sftp_path: The sftp remote path. For a single file it must include the
        file path. For multiple files it is the directory path where the files are
        located.
    :param sftp_filenames: Only used if you want to move multiple files. You can pass
        a list with exact filenames present in the sftp path, or a prefix that all
        files must match. Use ``"*"`` to move all files within the sftp path.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :param s3_key: The targeted s3 key. For a single file it must include the file
        path. For multiple files it must end with ``"/"``.
    :param s3_filenames: Only used if you want to move multiple files and name them
        differently from the originals on the SFTP server. It can be a list of
        filenames or a string prefix that replaces the sftp prefix.
    :param use_temp_file: If True, copies file first to local,
        if False streams file from SFTP to S3.
    :param fail_on_file_not_exist: If True, operator fails when file does not exist,
        if False, operator will not fail and skips transfer. Default is True.
    :param replace: If True, overwrite the S3 key if it already exists.
    :param encrypt: If True, the file will be encrypted on the server-side by S3.
    :param gzip: If True, the file will be compressed locally before upload.
    :param acl_policy: Canned ACL policy for the file being uploaded to S3.
    """

    template_fields: Sequence[str] = ("s3_key", "sftp_path", "s3_bucket", "sftp_filenames", "s3_filenames")

    def __init__(
        self,
        *,
        s3_bucket: str,
        s3_key: str,
        sftp_path: str,
        sftp_conn_id: str = "ssh_default",
        sftp_remote_host: str = "",
        sftp_filenames: str | list[str] | None = None,
        s3_filenames: str | list[str] | None = None,
        use_temp_file: bool = True,
        fail_on_file_not_exist: bool = True,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        aws_conn_id: str = "aws_default",
        s3_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if s3_conn_id is not None:
            warnings.warn(
                "The s3_conn_id parameter is deprecated. Use aws_conn_id instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            aws_conn_id = s3_conn_id
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.sftp_remote_host = sftp_remote_host
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_id = aws_conn_id
        self.sftp_filenames = sftp_filenames
        self.s3_filenames = s3_filenames
        self.use_temp_file = use_temp_file
        self.fail_on_file_not_exist = fail_on_file_not_exist
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    def _upload_to_s3(
        self,
        sftp_client: paramiko.SFTPClient,
        s3_hook: S3Hook,
        sftp_path: str,
        s3_key: str,
    ) -> None:
        try:
            sftp_client.stat(sftp_path)
        except FileNotFoundError:
            if self.fail_on_file_not_exist:
                raise
            self.log.info("File %s not found on SFTP server. Skipping transfer.", sftp_path)
            return

        if self.use_temp_file:
            with NamedTemporaryFile("w") as f:
                sftp_client.get(sftp_path, f.name)
                s3_hook.load_file(
                    filename=f.name,
                    key=s3_key,
                    bucket_name=self.s3_bucket,
                    replace=self.replace,
                    encrypt=self.encrypt,
                    gzip=self.gzip,
                    acl_policy=self.acl_policy,
                )
        else:
            extra_args: dict = {}
            if self.encrypt:
                extra_args["ServerSideEncryption"] = "AES256"
            if self.acl_policy:
                extra_args["ACL"] = self.acl_policy
            with sftp_client.file(sftp_path, mode="rb") as data:
                s3_hook.get_conn().upload_fileobj(
                    data, self.s3_bucket, s3_key, ExtraArgs=extra_args or None, Callback=self.log.info
                )

    def execute(self, context: Context) -> None:
        self.s3_key = self.get_s3_key(self.s3_key)

        # SSHHook will handle a None/"" sftp_remote_host
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id, remote_host=self.sftp_remote_host)
        s3_hook = S3Hook(self.aws_conn_id)
        sftp_client = ssh_hook.get_conn().open_sftp()

        if self.sftp_filenames:
            if isinstance(self.sftp_filenames, str):
                self.log.info("Getting files in %s", self.sftp_path)
                list_dir = sftp_client.listdir(self.sftp_path)
                if self.sftp_filenames == "*":
                    files = list_dir
                else:
                    sftp_prefix: str = self.sftp_filenames
                    files = [f for f in list_dir if sftp_prefix in f]

                for file in files:
                    self.log.info("Moving file %s", file)
                    if self.s3_filenames and isinstance(self.s3_filenames, str):
                        s3_filename = file.replace(self.sftp_filenames, self.s3_filenames)
                    else:
                        s3_filename = file
                    self._upload_to_s3(
                        sftp_client,
                        s3_hook,
                        f"{self.sftp_path}/{file}",
                        f"{self.s3_key}{s3_filename}",
                    )
            else:
                if self.s3_filenames:
                    for sftp_file, s3_file in zip(self.sftp_filenames, self.s3_filenames):
                        self._upload_to_s3(
                            sftp_client,
                            s3_hook,
                            self.sftp_path + sftp_file,
                            self.s3_key + s3_file,
                        )
                else:
                    for sftp_file in self.sftp_filenames:
                        self._upload_to_s3(
                            sftp_client,
                            s3_hook,
                            self.sftp_path + sftp_file,
                            self.s3_key + sftp_file,
                        )
        else:
            self._upload_to_s3(sftp_client, s3_hook, self.sftp_path, self.s3_key)
