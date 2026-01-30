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
    :param s3_key: The targeted s3 key. This is the specified file path for
        downloading the file from S3.
    :param ftp_path: The ftp remote path. This is the specified file path for
        uploading file to the FTP server.
    :param aws_conn_id: reference to a specific AWS connection
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    """

    template_fields: Sequence[str] = ("s3_bucket", "s3_key", "ftp_path")

    def __init__(
        self,
        *,
        s3_bucket,
        s3_key,
        ftp_path,
        aws_conn_id="aws_default",
        ftp_conn_id="ftp_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ftp_path = ftp_path
        self.aws_conn_id = aws_conn_id
        self.ftp_conn_id = ftp_conn_id

    def execute(self, context: Context):
        s3_hook = S3Hook(self.aws_conn_id)
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)

        s3_obj = s3_hook.get_key(self.s3_key, self.s3_bucket)

        with NamedTemporaryFile() as local_tmp_file:
            self.log.info("Downloading file from %s", self.s3_key)
            s3_obj.download_fileobj(local_tmp_file)
            local_tmp_file.seek(0)
            ftp_hook.store_file(self.ftp_path, local_tmp_file.name)
            self.log.info("File stored in %s", {self.ftp_path})
