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


class FTPToS3Operator(BaseOperator):
    """
    Transfer of one or more files from an FTP server to S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FTPToS3Operator`

    :param ftp_path: The ftp remote path. For one file it is mandatory to include the file as well.
        For multiple files, it is the route where the files will be found.
    :param s3_bucket: The targeted s3 bucket in which to upload the file(s).
    :param s3_key: The targeted s3 key. For one file it must include the file path. For several,
        it must end with "/".
    :param ftp_conn_id: The ftp connection id. The name or identifier for
        establishing a connection to the FTP server.
    :param aws_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3.
    :param replace: A flag to decide whether or not to overwrite the key
        if it already exists. If replace is False and the key exists, an
        error will be raised.
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :param gzip: If True, the file will be compressed locally
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    """

    template_fields: Sequence[str] = (
        "ftp_path",
        "s3_bucket",
        "s3_key",
    )

    def __init__(
        self,
        *,
        ftp_path: str,
        s3_bucket: str,
        s3_key: str,
        ftp_conn_id: str = "ftp_default",
        aws_conn_id: str | None = "aws_default",
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ftp_path = ftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ftp_conn_id = ftp_conn_id
        self.aws_conn_id = aws_conn_id
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

    def execute(self, context: Context):
        """Transfer file from an FTP site to S3."""
        ftp_hook = FTPHook(ftp_conn_id=self.ftp_conn_id)
        s3_hook = S3Hook(self.aws_conn_id)

        with NamedTemporaryFile() as local_tmp_file:
            # Write the file to local storage before writing the persisted file to S3
            ftp_hook.retrieve_file(
                remote_full_path=self.ftp_path, local_full_path_or_buffer=local_tmp_file.name
            )

            s3_hook.load_file(
                filename=local_tmp_file.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=self.replace,
                encrypt=self.encrypt,
                gzip=self.gzip,
                acl_policy=self.acl_policy,
            )
            self.log.info("File upload to %s", self.s3_key)
