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

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3CopyObjectOperator(BaseOperator):
    """
    Creates a copy of an object that is already stored in S3.

    Note: the S3 connection used here needs to have access to both
    source and destination bucket/key.

    :param source_bucket_key: The key of the source object.

        It can be either full s3:// style url or relative path from root level.

        When it's specified as a full s3:// url, please omit source_bucket_name.
    :type source_bucket_key: str
    :param dest_bucket_key: The key of the object to copy to.

        The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
    :type dest_bucket_key: str
    :param source_bucket_name: Name of the S3 bucket where the source object is in.

        It should be omitted when `source_bucket_key` is provided as a full s3:// url.
    :type source_bucket_name: str
    :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

        It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
    :type dest_bucket_name: str
    :param source_version_id: Version ID of the source object (OPTIONAL)
    :type source_version_id: str
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - False: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    @apply_defaults
    def __init__(
            self,
            source_bucket_key,
            dest_bucket_key,
            source_bucket_name=None,
            dest_bucket_name=None,
            source_version_id=None,
            aws_conn_id='aws_default',
            verify=None,
            *args, **kwargs):
        super(S3CopyObjectOperator, self).__init__(*args, **kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_hook.copy_object(self.source_bucket_key, self.dest_bucket_key,
                            self.source_bucket_name, self.dest_bucket_name,
                            self.source_version_id)
