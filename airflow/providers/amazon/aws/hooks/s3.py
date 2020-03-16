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

# pylint: disable=invalid-name
"""
Interact with AWS S3, using the boto3 library.
"""
import fnmatch
import gzip as gz
import io
import re
import shutil
from functools import wraps
from inspect import signature
from tempfile import NamedTemporaryFile
from typing import Optional
from urllib.parse import urlparse

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import chunks


def provide_bucket_name(func):
    """
    Function decorator that provides a bucket name taken from the connection
    in case no bucket name has been passed to the function.
    """

    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound_args = function_signature.bind(*args, **kwargs)

        if 'bucket_name' not in bound_args.arguments:
            self = args[0]
            if self.aws_conn_id:
                connection = self.get_connection(self.aws_conn_id)
                if connection.schema:
                    bound_args.arguments['bucket_name'] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


def unify_bucket_name_and_key(func):
    """
    Function decorator that unifies bucket name and key taken from the key
    in case no bucket name and at least a key has been passed to the function.
    """

    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound_args = function_signature.bind(*args, **kwargs)

        def get_key_name():
            if 'wildcard_key' in bound_args.arguments:
                return 'wildcard_key'
            if 'key' in bound_args.arguments:
                return 'key'
            raise ValueError('Missing key parameter!')

        key_name = get_key_name()
        if key_name and 'bucket_name' not in bound_args.arguments:
            bound_args.arguments['bucket_name'], bound_args.arguments[key_name] = \
                S3Hook.parse_s3_url(bound_args.arguments[key_name])

        return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


class S3Hook(AwsBaseHook):
    """
    Interact with AWS S3, using the boto3 library.
    """

    def get_conn(self):
        return self.get_client_type('s3')

    @staticmethod
    def parse_s3_url(s3url):
        """
        Parses the S3 Url into a bucket name and key.

        :param s3url: The S3 Url to parse.
        :rtype s3url: str
        :return: the parsed bucket name and key
        :rtype: tuple of str
        """
        parsed_url = urlparse(s3url)

        if not parsed_url.netloc:
            raise AirflowException('Please provide a bucket_name instead of "{s3url}"'.format(s3url=s3url))

        bucket_name = parsed_url.netloc
        key = parsed_url.path.strip('/')

        return bucket_name, key

    @provide_bucket_name
    def check_for_bucket(self, bucket_name=None):
        """
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: True if it exists and False if not.
        :rtype: bool
        """
        try:
            self.get_conn().head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            self.log.info(e.response["Error"]["Message"])
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name=None):
        """
        Returns a boto3.S3.Bucket object

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: the bucket object to the bucket name.
        :rtype: boto3.S3.Bucket
        """
        s3_resource = self.get_resource_type('s3')
        return s3_resource.Bucket(bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name=None, region_name=None):
        """
        Creates an Amazon S3 bucket.

        :param bucket_name: The name of the bucket
        :type bucket_name: str
        :param region_name: The name of the aws region in which to create the bucket.
        :type region_name: str
        """
        s3_conn = self.get_conn()
        if not region_name:
            region_name = s3_conn.meta.region_name
        if region_name == 'us-east-1':
            self.get_conn().create_bucket(Bucket=bucket_name)
        else:
            self.get_conn().create_bucket(Bucket=bucket_name,
                                          CreateBucketConfiguration={
                                              'LocationConstraint': region_name
                                          })

    @provide_bucket_name
    def check_for_prefix(self, prefix, delimiter, bucket_name=None):
        """
        Checks that a prefix exists in a bucket

        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param prefix: a key prefix
        :type prefix: str
        :param delimiter: the delimiter marks key hierarchy.
        :type delimiter: str
        :return: False if the prefix does not exist in the bucket and True if it does.
        :rtype: bool
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(r'(\w+[{d}])$'.format(d=delimiter), prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return False if plist is None else prefix in plist

    @provide_bucket_name
    def list_prefixes(self, bucket_name=None, prefix='', delimiter='',
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
        :return: a list of matched prefixes and None if there are none.
        :rtype: list
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
                for common_prefix in page['CommonPrefixes']:
                    prefixes.append(common_prefix['Prefix'])

        if has_results:
            return prefixes
        return None

    @provide_bucket_name
    def list_keys(self, bucket_name=None, prefix='', delimiter='',
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
        :return: a list of matched keys and None if there are none.
        :rtype: list
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
        return None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_key(self, key, bucket_name=None):
        """
        Checks if a key exists in a bucket

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: True if the key exists and False if not.
        :rtype: bool
        """

        try:
            self.get_conn().head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            self.log.info(e.response["Error"]["Message"])
            return False

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_key(self, key, bucket_name=None):
        """
        Returns a boto3.s3.Object

        :param key: the path to the key
        :type key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :return: the key object from the bucket
        :rtype: boto3.s3.Object
        """

        obj = self.get_resource_type('s3').Object(bucket_name, key)
        obj.load()
        return obj

    @provide_bucket_name
    @unify_bucket_name_and_key
    def read_key(self, key, bucket_name=None):
        """
        Reads a key from S3

        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: the content of the key
        :rtype: boto3.s3.Object
        """

        obj = self.get_key(key, bucket_name)
        return obj.get()['Body'].read().decode('utf-8')

    @provide_bucket_name
    @unify_bucket_name_and_key
    def select_key(self, key, bucket_name=None,
                   expression='SELECT * FROM S3Object',
                   expression_type='SQL',
                   input_serialization=None,
                   output_serialization=None):
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
        if input_serialization is None:
            input_serialization = {'CSV': {}}
        if output_serialization is None:
            output_serialization = {'CSV': {}}

        response = self.get_conn().select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=expression,
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization)

        return ''.join(event['Records']['Payload'].decode('utf-8')
                       for event in response['Payload']
                       if 'Records' in event)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_wildcard_key(self,
                               wildcard_key, bucket_name=None, delimiter=''):
        """
        Checks that a key matching a wildcard expression exists in a bucket

        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        return self.get_wildcard_key(wildcard_key=wildcard_key,
                                     bucket_name=bucket_name,
                                     delimiter=delimiter) is not None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_wildcard_key(self, wildcard_key, bucket_name=None, delimiter=''):
        """
        Returns a boto3.s3.Object object matching the wildcard expression

        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: the key object from the bucket or None if none has been found.
        :rtype: boto3.s3.Object
        """

        prefix = re.split(r'[*]', wildcard_key, 1)[0]
        key_list = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        if key_list:
            key_matches = [k for k in key_list if fnmatch.fnmatch(k, wildcard_key)]
            if key_matches:
                return self.get_key(key_matches[0], bucket_name)
        return None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file(self,
                  filename,
                  key,
                  bucket_name=None,
                  replace=False,
                  encrypt=False,
                  gzip=False):
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
        :param gzip: If True, the file will be compressed locally
        :type gzip: bool
        """

        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if gzip:
            filename_gz = filename.name + '.gz'
            with open(filename.name, 'rb') as f_in:
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args)

    @provide_bucket_name
    @unify_bucket_name_and_key
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

        :param string_data: str to set as content for the key.
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
        :param encoding: The string to byte encoding
        :type encoding: str
        """
        bytes_data = string_data.encode(encoding)
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt)

    @provide_bucket_name
    @unify_bucket_name_and_key
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
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file_obj(self,
                      file_obj,
                      key,
                      bucket_name=None,
                      replace=False,
                      encrypt=False):
        """
        Loads a file object to S3

        :param file_obj: The file-like object to set as the content for the S3 key.
        :type file_obj: file-like object
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which to store the file
        :type bucket_name: str
        :param replace: A flag that indicates whether to overwrite the key
            if it already exists.
        :type replace: bool
        :param encrypt: If True, S3 encrypts the file on the server,
            and the file is stored in encrypted form at rest in S3.
        :type encrypt: bool
        """
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt)

    def _upload_file_obj(self,
                         file_obj,
                         key,
                         bucket_name=None,
                         replace=False,
                         encrypt=False):
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError("The key {key} already exists.".format(key=key))

        extra_args = {}
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"

        client = self.get_conn()
        client.upload_fileobj(file_obj, bucket_name, key, ExtraArgs=extra_args)

    def copy_object(self,
                    source_bucket_key,
                    dest_bucket_key,
                    source_bucket_name=None,
                    dest_bucket_name=None,
                    source_version_id=None):
        """
        Creates a copy of an object that is already stored in S3.

        Note: the S3 connection used here needs to have access to both
        source and destination bucket/key.

        :param source_bucket_key: The key of the source object.

            It can be either full s3:// style url or relative path from root level.

            When it's specified as a full s3:// url, please omit source_bucket_name.
        :type source_bucket_key: str
        :param dest_bucket_key: The key of the object to copy to.

            The convention to specify `dest_bucket_key` is the same
            as `source_bucket_key`.
        :type dest_bucket_key: str
        :param source_bucket_name: Name of the S3 bucket where the source object is in.

            It should be omitted when `source_bucket_key` is provided as a full s3:// url.
        :type source_bucket_name: str
        :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

            It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
        :type dest_bucket_name: str
        :param source_version_id: Version ID of the source object (OPTIONAL)
        :type source_version_id: str
        """

        if dest_bucket_name is None:
            dest_bucket_name, dest_bucket_key = self.parse_s3_url(dest_bucket_key)
        else:
            parsed_url = urlparse(dest_bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException('If dest_bucket_name is provided, ' +
                                       'dest_bucket_key should be relative path ' +
                                       'from root level, rather than a full s3:// url')

        if source_bucket_name is None:
            source_bucket_name, source_bucket_key = self.parse_s3_url(source_bucket_key)
        else:
            parsed_url = urlparse(source_bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException('If source_bucket_name is provided, ' +
                                       'source_bucket_key should be relative path ' +
                                       'from root level, rather than a full s3:// url')

        copy_source = {'Bucket': source_bucket_name,
                       'Key': source_bucket_key,
                       'VersionId': source_version_id}
        response = self.get_conn().copy_object(Bucket=dest_bucket_name,
                                               Key=dest_bucket_key,
                                               CopySource=copy_source)
        return response

    def delete_objects(self, bucket, keys):
        """
        Delete keys from the bucket.

        :param bucket: Name of the bucket in which you are going to delete object(s)
        :type bucket: str
        :param keys: The key(s) to delete from S3 bucket.

            When ``keys`` is a string, it's supposed to be the key name of
            the single object to delete.

            When ``keys`` is a list, it's supposed to be the list of the
            keys to delete.
        :type keys: str or list
        """
        if isinstance(keys, str):
            keys = [keys]

        s3 = self.get_conn()

        # We can only send a maximum of 1000 keys per request.
        # For details see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        for chunk in chunks(keys, chunk_size=1000):
            response = s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk]}
            )
            deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
            self.log.info("Deleted: %s", deleted_keys)
            if "Errors" in response:
                errors_keys = [x['Key'] for x in response.get("Errors", [])]
                raise AirflowException("Errors when deleting: {}".format(errors_keys))

    @provide_bucket_name
    @unify_bucket_name_and_key
    def download_file(
        self,
        key: str,
        bucket_name: Optional[str] = None,
        local_path: Optional[str] = None
    ) -> str:
        """
        Downloads a file from the S3 location to the local file system.

        :param key: The key path in S3.
        :type key: str
        :param bucket_name: The specific bucket to use.
        :type bucket_name: Optional[str]
        :param local_path: The local path to the downloaded file. If no path is provided it will use the
            system's temporary directory.
        :type local_path: Optional[str]
        :return: the file name.
        :rtype: str
        """
        self.log.info('Downloading source S3 file from Bucket %s with path %s', bucket_name, key)

        if not self.check_for_key(key, bucket_name):
            raise AirflowException(f'The source file in Bucket {bucket_name} with path {key} does not exist')

        s3_obj = self.get_key(key, bucket_name)

        with NamedTemporaryFile(dir=local_path, prefix='airflow_tmp_', delete=False) as local_tmp_file:
            s3_obj.download_fileobj(local_tmp_file)

        return local_tmp_file.name
