# -*- coding: utf-8 -*-
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

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.ssh_hook import SSHHook
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse
from airflow.utils.decorators import apply_defaults


class S3ToSFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a SFTP server
    :param sftp_conn_id:    The sftp connection id. The name or
                            identifier for establishing a connection to the SFTP server.
    :type sftp_conn_id:     string
    :param sftp_path:       The sftp remote path. This is the specified
                            file path for uploading file to the SFTP server.
    :type sftp_path:        string
    :param s3_conn_id:      The s3 connnection id. The name or identifier for establishing
                            a connection to S3
    :type s3_conn_id:       string
    :param s3_bucket:       The targeted s3 bucket. This is the S3 bucket
                            from where the file is downloaded.
    :type s3_bucket:        string
    :param s3_key:          The targeted s3 key. This is the specified file path
                            for downloading the file from S3.
    :type s3_key:           string
    """

    template_fields = ('s3_key', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 sftp_path,
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(S3ToSFTPOperator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

    @staticmethod
    def get_s3_key(s3_key):
        """This parses the correct format for S3 keys
            regardless of how the S3 url is passed."""

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):
        self.s3_key = self.get_s3_key(self.s3_key)
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        s3_client = s3_hook.get_conn()
        sftp_client = ssh_hook.get_conn().open_sftp()

        with NamedTemporaryFile("w") as f:
            s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
            sftp_client.put(f.name, self.sftp_path)
