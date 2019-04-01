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
    Create copy of objects from S3 to S3.

    Note: the S3 connection used here needs to have access to both
    source and destination buckets.

    :param source_bucket_key: The list of source object keys. (templated)
        It can be either str or list of str, 
        Keys must be relative path from root level.
        Full s3:// url will returns a error.
    :type source_bucket_key: str or list

    :param dest_bucket_key: The key of the object to copy to. (templated)
        You can only specify a single key. 
        If there are multiple sources only first file will be copied.
    :type dest_bucket_key: str
    
    :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)
        It is a required field.
    :type source_bucket_name: str

    :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)
        It is a required field.
    :type dest_bucket_name: str
    
    :param source_bucket_prefix: prifix of source object keys. (templated)
        default prefix is empty string which will accept all files from the source bucket, 
        prefix will only execute if source_bucket_key is not specified.
    :type source_bucket_prefix: str

    :param source_bucket_delimiter:delimiter of source object keys. (templated)
        The convention to specify `source_bucket_delimiter` is the same as `source_bucket_prefix`.
    :type source_bucket_delimiter: str

    :param dest_bucket_path: destination path where to copy. (templated)
        Destination path must be relative path from root level.
        Uses same file name as source.
        For same path as source please omit dest_bucket_path along with source_bucket_key.
    :type source_bucket_path: str

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

    template_fields = ('source_bucket_key', 'dest_bucket_key',
                       'source_bucket_name', 'dest_bucket_name',
                       'source_bucket_prefix', 'source_bucket_delimiter',
                       'dest_bucket_path')

    @apply_defaults
    def __init__(
            self,
            source_bucket_key=None,
            dest_bucket_key=None,
            source_bucket_name,
            dest_bucket_name,
            source_bucket_prefix='',
            source_bucket_delimiter='',
            dest_bucket_path=None,
            source_version_id=None,
            aws_conn_id='aws_default',
            verify=None,
            *args, **kwargs):
        super(S3CopyObjectOperator, self).__init__(*args, **kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_bucket_prefix = source_bucket_prefix
        self.source_bucket_delimiter = source_bucket_delimiter
        self.dest_bucket_path = dest_bucket_path
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        if self.source_bucket_key is None:
            self.log.info(
                'Getting the list of files from bucket: %s in prefix: %s (Delimiter %s)',
                self.source_bucket_name, self.source_bucket_prefix, self.source_bucket_delimiter
            )

            self.source_bucket_key = s3_hook.list_keys(
                    bucket_name=self.source_bucket_name,
                    prefix=self.source_bucket_prefix,
                    delimiter=self.source_bucket_delimiter)
        
        elif not isinstance(self.source_bucket_key,list):
            self.source_bucket_key = [self.source_bucket_key]

        if self.source_bucket_key is None:
            self.log.info(
                "No files to copy!"
            )
        
        elif(self.dest_bucket_key is not None):
            self.log.info(
                'Copying file: s3://%s/%s to s3://%s/%s',
                self.source_bucket_name, self.source_bucket_key[0], 
                self.dest_bucket_name, self.dest_bucket_key
            )
        
            s3_hook.copy_object(self.source_bucket_key[0], self.dest_bucket_key,
                self.source_bucket_name, self.dest_bucket_name,
                self.source_version_id)

            self.log.info(
                'All done, 1 file Copied successfully!',
            )
                    
        elif(self.dest_bucket_path is None):
            for key in self.source_bucket_key:
                self.log.info(
                    'Copying file: s3://%s/%s to s3://%s/%s',
                    self.source_bucket_name, key, self.dest_bucket_name, key
                )
            
                s3_hook.copy_object(key, key,
                                self.source_bucket_name, self.dest_bucket_name,
                                self.source_version_id)

            self.log.info(
                'All done, %d file(s) Copied successfully!', len(self.source_bucket_key)
            )   

        else:
            if(self.dest_bucket_path is not "" and not self.dest_bucket_path.endswith("/")):
                self.dest_bucket_path=self.dest_bucket_path+"/"

            for key in self.source_bucket_key:
                dest = self.dest_bucket_path + key[key.rfind('/') + 1:]
 
                self.log.info(
                    'Copying file: s3://%s/%s to s3://%s/%s',
                    self.source_bucket_name, key, self.dest_bucket_name, dest
                )
            
                s3_hook.copy_object(key, dest,
                                self.source_bucket_name, self.dest_bucket_name,
                                self.source_version_id)
            

            self.log.info(
                'All done,%d file(s) Copied successfully!', len(self.source_bucket_key)
            )
