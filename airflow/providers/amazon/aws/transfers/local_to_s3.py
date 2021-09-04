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
from typing import Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class LocalFilesystemToS3Operator(BaseOperator):
    """
    Uploads a file from a local filesystem to Amazon S3.

    :param filename: Path to the local file. Path can be either absolute
            (e.g. /path/to/file.ext) or relative (e.g. ../../foo/*/*.csv). (templated)
    :type filename: str
    :param dest_key: The key of the object to copy to. (templated)

        It can be either full s3:// style url or relative path from root level.

        When it's specified as a full s3:// url, including dest_bucket results in a TypeError.
    :type dest_key: str
    :param dest_bucket: Name of the S3 bucket to where the object is copied. (templated)

        Inclusion when `dest_key` is provided as a full s3:// url results in a TypeError.
    :type dest_bucket: str
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
    :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
    :type replace: bool
    :param encrypt: If True, the file will be encrypted on the server-side
        by S3 and will be stored in an encrypted form while at rest in S3.
    :type encrypt: bool
    :param gzip: If True, the file will be compressed locally
    :type gzip: bool
    :param acl_policy: String specifying the canned ACL policy for the file being
        uploaded to the S3 bucket.
    :type acl_policy: str
    """

    template_fields = ('filename', 'dest_key', 'dest_bucket')

    def __init__(
        self,
        *,
        filename: str,
        dest_key: str,
        dest_bucket: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.filename = filename
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.replace = replace
        self.encrypt = encrypt
        self.gzip = gzip
        self.acl_policy = acl_policy

        if 's3://' in self.dest_key and self.dest_bucket is not None:
            raise TypeError('dest_bucket should be None when dest_key is provided as a full s3:// file path.')

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_hook.load_file(
            self.filename,
            self.dest_key,
            self.dest_bucket,
            self.replace,
            self.encrypt,
            self.gzip,
            self.acl_policy,
        )
