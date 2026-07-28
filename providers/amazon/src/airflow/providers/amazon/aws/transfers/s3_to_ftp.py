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

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils import validate_destination_path
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class S3ToFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a FTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToFTPOperator`

    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
        where the file is downloaded.
    :param s3_key: The targeted s3 key. For a single file it must include the file
        path. For multiple files it is the key prefix (directory) and must end with
        ``"/"``.
    :param s3_filenames: Only used if you want to move multiple files. You can pass
        a list with exact key suffixes present under the s3_key prefix, or a string
        prefix that all filenames must match. Use ``"*"`` to move all objects under
        the s3_key prefix.
    :param ftp_path: The ftp remote path. For a single file it must include the file
        path. For multiple files it is the destination directory path and must end
        with ``"/"``.
    :param ftp_filenames: Only used if you want to move multiple files and name them
        differently at the destination. It can be a list of filenames or a string
        prefix that replaces the s3 prefix.
    :param aws_conn_id: reference to a specific AWS connection
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :param fail_on_file_not_exist: If True, operator fails when a source S3 key does not
        exist. If False, the operator logs a warning and skips the transfer. Default is True.
    """

    template_fields: Sequence[str] = ("s3_bucket", "s3_key", "ftp_path", "s3_filenames", "ftp_filenames")

    def __init__(
        self,
        *,
        s3_bucket,
        s3_key,
        ftp_path,
        s3_filenames: str | list[str] | None = None,
        ftp_filenames: str | list[str] | None = None,
        aws_conn_id="aws_default",
        ftp_conn_id="ftp_default",
        fail_on_file_not_exist: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ftp_path = ftp_path
        self.s3_filenames = s3_filenames
        self.ftp_filenames = ftp_filenames
        self.aws_conn_id = aws_conn_id
        self.ftp_conn_id = ftp_conn_id
        self.fail_on_file_not_exist = fail_on_file_not_exist

    def _download_from_s3(self, s3_hook: S3Hook, ftp_hook: FTPHook, s3_key: str, ftp_path: str) -> None:
        validate_destination_path(ftp_path, self.ftp_path, base_name="ftp_path")
        if not s3_hook.check_for_key(s3_key, self.s3_bucket):
            if self.fail_on_file_not_exist:
                raise FileNotFoundError(f"Key {s3_key!r} not found in S3 bucket {self.s3_bucket!r}")
            self.log.info("Key %s not found in S3. Skipping transfer.", s3_key)
            return
        s3_obj = s3_hook.get_key(s3_key, self.s3_bucket)
        with NamedTemporaryFile() as local_tmp_file:
            self.log.info("Downloading file from %s", s3_key)
            s3_obj.download_fileobj(local_tmp_file)
            local_tmp_file.seek(0)
            ftp_hook.store_file(ftp_path, local_tmp_file.name)
            self.log.info("File stored in %s", ftp_path)

    def execute(self, context: Context):
        s3_hook = S3Hook(self.aws_conn_id)
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

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
                    if self.ftp_filenames and isinstance(self.ftp_filenames, str):
                        ftp_filename = file.replace(self.s3_filenames, self.ftp_filenames)
                    else:
                        ftp_filename = file
                    self._download_from_s3(
                        s3_hook, ftp_hook, self.s3_key + file, self.ftp_path + ftp_filename
                    )
            else:
                if self.ftp_filenames:
                    for s3_file, ftp_file in zip(self.s3_filenames, self.ftp_filenames):
                        self._download_from_s3(
                            s3_hook, ftp_hook, self.s3_key + s3_file, self.ftp_path + ftp_file
                        )
                else:
                    for s3_file in self.s3_filenames:
                        self._download_from_s3(
                            s3_hook, ftp_hook, self.s3_key + s3_file, self.ftp_path + s3_file
                        )
        else:
            self._download_from_s3(s3_hook, ftp_hook, self.s3_key, self.ftp_path)
