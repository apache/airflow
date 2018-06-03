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

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook

from six import BytesIO
from urllib.parse import urlparse
import re
import fnmatch


class S3Hook(AwsHook):
    """
    Interact with AWS S3, using the boto3 library.
    """

    def get_conn(self):
        return self.get_client_type('s3')

    @staticmethod
    def parse_s3_url(s3url):
        parsed_url = urlparse(s3url)
        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name instead of "%s"' % s3url)
        else:
            bucket_name = parsed_url.netloc
            key = parsed_url.path.strip('/')
            return (bucket_name, key)

    def check_for_bucket(self, bucket_name):
        """
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        try:
            self.get_conn().head_bucket(Bucket=bucket_name)
            return True
        except:
            return False

    def get_bucket(self, bucket_name):
        """
        Returns a boto3.S3.Bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        s3 = self.get_resource_type('s3')
        return s3.Bucket(bucket_name)

    def check_for_prefix(self, bucket_name, prefix, delimiter):
        """
        Checks that a prefix exists in a bucket
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(r'(\w+[{d}])$'.format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return False if plist is None else prefix in plist

    def list_prefixes(self, bucket_name, prefix='', delimiter='',
                      page_size=None, max_items=None):
        """
        Lists prefixes in a bucket under prefix

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(Bucket=bucket_name,
                                      Prefix=prefix,
                                      Delimiter=delimiter,
                                      PaginationConfig=config)

        has_results = False
        prefixes = []
        for page in response:
            if 'CommonPrefixes' in page:
                has_results = True
                for p in page['CommonPrefixes']:
                    prefixes.append(p['Prefix'])

        if has_results:
            return prefixes

    def list_keys(self, bucket_name, prefix='', delimiter='',
                  page_size=None, max_items=None):
        """
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :param page_size: pagination size
        :type page_size: int
        :param max_items: maximum items to return
        :type max_items: int
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(Bucket=bucket_name,
                                      Prefix=prefix,
                                      Delimiter=delimiter,
                                      PaginationConfig=config)

        has_results = False
        keys = []
        for page in response:
            if 'Contents' in page:
                has_results = True
                for k in page['Contents']:
                    keys.append(k['Key'])

        if has_results:
            return keys

    def check_for_key(self, key, bucket_name=None):
        """
        Checks if a key exists in a bucket

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        try:
            self.get_conn().head_object(Bucket=bucket_name, Key=key)
            return True
        except:
            return False

    def get_key(self, key, bucket_name=None):
        """
        Returns a boto3.s3.Object

        :param key: the path to the key
        :type key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        obj = self.get_resource_type('s3').Object(bucket_name, key)
        obj.load()
        return obj

    def read_key(self, key, bucket_name=None):
        """
        Reads a key from S3

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        """

        obj = self.get_key(key, bucket_name)
        return obj.get()['Body'].read().decode('utf-8')

    def select_key(self, key, bucket_name=None,
                   expression='SELECT * FROM S3Object',
                   expression_type='SQL',
                   input_serialization={'CSV': {}},
                   output_serialization={'CSV': {}}):
        """
        Reads a key with S3 Select.

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :param expression: S3 Select expression
        :type expression: str
        :param expression_type: S3 Select expression type
        :type expression_type: str
        :param input_serialization: S3 Select input data serialization format
        :type input_serialization: dict
        :param output_serialization: S3 Select output data serialization format
        :type output_serialization: dict
        :return: retrieved subset of original data by S3 Select
        :rtype: str

        .. seealso::
            For more details about S3 Select parameters:
            http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.select_object_content
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        response = self.get_conn().select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=expression,
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization)

        return ''.join(event['Records']['Payload']
                       for event in response['Payload']
                       if 'Records' in event)

    def check_for_wildcard_key(self,
                               wildcard_key, bucket_name=None, delimiter=''):
        """
        Checks that a key matching a wildcard expression exists in a bucket
        """
        return self.get_wildcard_key(wildcard_key=wildcard_key,
                                     bucket_name=bucket_name,
                                     delimiter=delimiter) is not None

    def get_wildcard_key(self, wildcard_key, bucket_name=None, delimiter=''):
        """
        Returns a boto3.s3.Object object matching the wildcard expression

        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        """
        if not bucket_name:
            (bucket_name, wildcard_key) = self.parse_s3_url(wildcard_key)

        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        klist = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if klist:
            key_matches = [k for k in klist if fnmatch.fnmatch(k, wildcard_key)]
            if key_matches:
                return self.get_key(key_matches[0], bucket_name)

    def load_file(self,
                  filename,
                  key,
                  bucket_name=None,
                  replace=False,
                  encrypt=False):
        """
        Loads a local file to S3

        :param filename: name of the file to load.
        :type filename: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)

    def load_string(self,
                    string_data,
                    key,
                    bucket_name=None,
                    replace=False,
                    encrypt=False,
                    encoding='utf-8'):
        """
        Loads a string to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        :param string_data: string to set as content for the key.
        :type string_data: str
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        """
        self.load_bytes(string_data.encode(encoding),
                        key=key,
                        bucket_name=bucket_name,
                        replace=replace,
                        encrypt=encrypt)

    def load_bytes(self,
                   bytes_data,
                   key,
                   bucket_name=None,
                   replace=False,
                   encrypt=False):
        """
        Loads bytes to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        :param bytes_data: bytes to set as content for the key.
        :type bytes_data: bytes
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :type replace: bool
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :type encrypt: bool
        """
        if not bucket_name:
            (bucket_name, key) = self.parse_s3_url(key)

        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"

        filelike_buffer = BytesIO(bytes_data)

        client = self.get_conn()
        client.upload_fileobj(filelike_buffer, bucket_name, key, ExtraArgs=extra_args)
