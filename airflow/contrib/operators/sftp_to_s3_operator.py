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


def get_s3_key(s3_key):
    """This parses the correct format for S3 keys
        regardless of how the S3 url is passed. """

    parsed_s3_key = urlparse(s3_key)
    return parsed_s3_key.path.lstrip('/')


class SFTPToS3Operator(BaseOperator):
    """
    S3 To SFTP Operator
    :param sftp_conn_id:    The sftp connection id.
    :type sftp_conn_id:     string
    :param sftp_path:       The sftp remote path.
    :type sftp_path:        string
    :param s3_conn_id:      The s3 connnection id.
    :type s3_conn_id:       string
    :param s3_bucket:       The targeted s3 bucket.
    :type s3_bucket:        string
    :param s3_key:          The targeted s3 key.
    :type s3_key:           string
    """

    template_fields = ('s3_key', 'sftp_path')

    def __init__(self,
                 sftp_conn_id=None,
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_key=None,
                 sftp_path=None,
                 *args,
                 **kwargs):
        super(SFTPToS3Operator, self).__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

        self.ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        self.s3_hook = S3Hook(self.s3_conn_id)

    def execute(self, context):
        self.s3_key = get_s3_key(self.s3_key)

        ssh_client = self.ssh_hook.get_conn()
        sftp_client = ssh_client.open_sftp()

        with NamedTemporaryFile("w") as f:
            sftp_client.get(self.sftp_path, f.name)

            self.s3_hook.load_file(
                filename=f.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )
