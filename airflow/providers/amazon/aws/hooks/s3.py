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
"""Interact with AWS S3, using the boto3 library."""
from __future__ import annotations

import fnmatch
import gzip as gz
import io
import re
import shutil
from copy import deepcopy
from datetime import datetime
from functools import wraps
from inspect import signature
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, List, TypeVar, cast
from urllib.parse import urlparse

from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import chunks

T = TypeVar("T", bound=Callable)


def provide_bucket_name(func: T) -> T:
    """
    Function decorator that provides a bucket name taken from the connection
    in case no bucket name has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        if 'bucket_name' not in bound_args.arguments:
            self = args[0]
            if self.aws_conn_id:
                connection = self.get_connection(self.aws_conn_id)
                if connection.schema:
                    bound_args.arguments['bucket_name'] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


def unify_bucket_name_and_key(func: T) -> T:
    """
    Function decorator that unifies bucket name and key taken from the key
    in case no bucket name and at least a key has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        if 'wildcard_key' in bound_args.arguments:
            key_name = 'wildcard_key'
        elif 'key' in bound_args.arguments:
            key_name = 'key'
        else:
            raise ValueError('Missing key parameter!')

        if 'bucket_name' not in bound_args.arguments:
            bound_args.arguments['bucket_name'], bound_args.arguments[key_name] = S3Hook.parse_s3_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class S3Hook(AwsBaseHook):
    """
    Interact with AWS S3, using the boto3 library.

    :param transfer_config_args: Configuration object for managed S3 transfers.
    :param extra_args: Extra arguments that may be passed to the download/upload operations.

    .. seealso::
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#s3-transfers

        - For allowed upload extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS``.
        - For allowed download extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS``.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    conn_type = 's3'
    hook_name = 'Amazon S3'

    def __init__(
        self,
        aws_conn_id: str | None = AwsBaseHook.default_conn_name,
        transfer_config_args: dict | None = None,
        extra_args: dict | None = None,
        *args,
        **kwargs,
    ) -> None:
        kwargs['client_type'] = 's3'
        kwargs['aws_conn_id'] = aws_conn_id

        if transfer_config_args and not isinstance(transfer_config_args, dict):
            raise TypeError(f"transfer_config_args expected dict, got {type(transfer_config_args).__name__}.")
        self.transfer_config = TransferConfig(**transfer_config_args or {})

        if extra_args and not isinstance(extra_args, dict):
            raise TypeError(f"extra_args expected dict, got {type(extra_args).__name__}.")
        self._extra_args = extra_args or {}

        super().__init__(*args, **kwargs)

    @property
    def extra_args(self):
        """Return hook's extra arguments (immutable)."""
        return deepcopy(self._extra_args)

    @staticmethod
    def parse_s3_url(s3url: str) -> tuple[str, str]:
        """
        Parses the S3 Url into a bucket name and key.

        :param s3url: The S3 Url to parse.
        :return: the parsed bucket name and key
        :rtype: tuple of str
        """
        parsed_url = urlparse(s3url)

        if not parsed_url.netloc:
            raise AirflowException(f'Please provide a bucket_name instead of "{s3url}"')

        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        return bucket_name, key

    @staticmethod
    def get_s3_bucket_key(
        bucket: str | None, key: str, bucket_param_name: str, key_param_name: str
    ) -> tuple[str, str]:
        """
        Get the S3 bucket name and key from either:
            - bucket name and key. Return the info as it is after checking `key` is a relative path
            - key. Must be a full s3:// url

        :param bucket: The S3 bucket name
        :param key: The S3 key
        :param bucket_param_name: The parameter name containing the bucket name
        :param key_param_name: The parameter name containing the key name
        :return: the parsed bucket name and key
        :rtype: tuple of str
        """
        if bucket is None:
            return S3Hook.parse_s3_url(key)

        parsed_url = urlparse(key)
        if parsed_url.scheme != '' or parsed_url.netloc != '':
            raise TypeError(
                f'If `{bucket_param_name}` is provided, {key_param_name} should be a relative path '
                'from root level, rather than a full s3:// url'
            )

        return bucket, key

    @provide_bucket_name
    def check_for_bucket(self, bucket_name: str | None = None) -> bool:
        """
        Check if bucket_name exists.

        :param bucket_name: the name of the bucket
        :return: True if it exists and False if not.
        :rtype: bool
        """
        try:
            self.get_conn().head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            # The head_bucket api is odd in that it cannot return proper
            # exception objects, so error codes must be used. Only 200, 404 and 403
            # are ever returned. See the following links for more details:
            # https://github.com/boto/boto3/issues/2499
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket
            return_code = int(e.response['Error']['Code'])
            if return_code == 404:
                self.log.error('Bucket "%s" does not exist', bucket_name)
            elif return_code == 403:
                self.log.error(
                    'Access to bucket "%s" is forbidden or there was an error with the request', bucket_name
                )
                self.log.error(e)
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name: str | None = None) -> object:
        """
        Returns a boto3.S3.Bucket object

        :param bucket_name: the name of the bucket
        :return: the bucket object to the bucket name.
        :rtype: boto3.S3.Bucket
        """
        s3_resource = self.get_session().resource(
            "s3",
            endpoint_url=self.conn_config.endpoint_url,
            config=self.config,
            verify=self.verify,
        )
        return s3_resource.Bucket(bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name: str | None = None, region_name: str | None = None) -> None:
        """
        Creates an Amazon S3 bucket.

        :param bucket_name: The name of the bucket
        :param region_name: The name of the aws region in which to create the bucket.
        """
        if not region_name:
            if self.conn_region_name == "aws-global":
                raise AirflowException(
                    "Unable to create bucket if `region_name` not set "
                    "and boto3 configured to use s3 regional endpoints."
                )
            region_name = self.conn_region_name

        if region_name == 'us-east-1':
            self.get_conn().create_bucket(Bucket=bucket_name)
        else:
            self.get_conn().create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region_name}
            )

    @provide_bucket_name
    def check_for_prefix(self, prefix: str, delimiter: str, bucket_name: str | None = None) -> bool:
        """
        Checks that a prefix exists in a bucket

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :return: False if the prefix does not exist in the bucket and True if it does.
        :rtype: bool
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(fr'(\w+[{delimiter}])$', prefix, 1)
        previous_level = prefix_split[0]
        plist = self.list_prefixes(bucket_name, previous_level, delimiter)
        return prefix in plist

    @provide_bucket_name
    def list_prefixes(
        self,
        bucket_name: str | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> list:
        """
        Lists prefixes in a bucket under prefix

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched prefixes
        :rtype: list
        """
        prefix = prefix or ''
        delimiter = delimiter or ''
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )

        prefixes = []  # type: List[str]
        for page in response:
            if 'CommonPrefixes' in page:
                prefixes.extend(common_prefix['Prefix'] for common_prefix in page['CommonPrefixes'])

        return prefixes

    def _list_key_object_filter(
        self, keys: list, from_datetime: datetime | None = None, to_datetime: datetime | None = None
    ) -> list:
        def _is_in_period(input_date: datetime) -> bool:
            if from_datetime is not None and input_date <= from_datetime:
                return False
            if to_datetime is not None and input_date > to_datetime:
                return False
            return True

        return [k['Key'] for k in keys if _is_in_period(k['LastModified'])]

    @provide_bucket_name
    def list_keys(
        self,
        bucket_name: str | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
        start_after_key: str | None = None,
        from_datetime: datetime | None = None,
        to_datetime: datetime | None = None,
        object_filter: Callable[..., list] | None = None,
    ) -> list:
        """
        Lists keys in a bucket under prefix and not containing delimiter

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :param start_after_key: should return only keys greater than this key
        :param from_datetime: should return only keys with LastModified attr greater than this equal
            from_datetime
        :param to_datetime: should return only keys with LastModified attr less than this to_datetime
        :param object_filter: Function that receives the list of the S3 objects, from_datetime and
            to_datetime and returns the List of matched key.

        **Example**: Returns the list of S3 object with LastModified attr greater than from_datetime
             and less than to_datetime:

        .. code-block:: python

            def object_filter(
                keys: list,
                from_datetime: Optional[datetime] = None,
                to_datetime: Optional[datetime] = None,
            ) -> list:
                def _is_in_period(input_date: datetime) -> bool:
                    if from_datetime is not None and input_date < from_datetime:
                        return False

                    if to_datetime is not None and input_date > to_datetime:
                        return False
                    return True

                return [k["Key"] for k in keys if _is_in_period(k["LastModified"])]

        :return: a list of matched keys
        :rtype: list
        """
        prefix = prefix or ''
        delimiter = delimiter or ''
        start_after_key = start_after_key or ''
        self.object_filter_usr = object_filter
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter=delimiter,
            PaginationConfig=config,
            StartAfter=start_after_key,
        )

        keys = []  # type: List[str]
        for page in response:
            if 'Contents' in page:
                keys.extend(iter(page['Contents']))
        if self.object_filter_usr is not None:
            return self.object_filter_usr(keys, from_datetime, to_datetime)

        return self._list_key_object_filter(keys, from_datetime, to_datetime)

    @provide_bucket_name
    def get_file_metadata(
        self,
        prefix: str,
        bucket_name: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> list:
        """
        Lists metadata objects in a bucket under prefix

        :param prefix: a key prefix
        :param bucket_name: the name of the bucket
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of metadata of objects
        :rtype: list
        """
        config = {
            'PageSize': page_size,
            'MaxItems': max_items,
        }

        paginator = self.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(Bucket=bucket_name, Prefix=prefix, PaginationConfig=config)

        files = []
        for page in response:
            if 'Contents' in page:
                files += page['Contents']
        return files

    @provide_bucket_name
    @unify_bucket_name_and_key
    def head_object(self, key: str, bucket_name: str | None = None) -> dict | None:
        """
        Retrieves metadata of an object

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: metadata of an object
        :rtype: dict
        """
        try:
            return self.get_conn().head_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            else:
                raise e

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_key(self, key: str, bucket_name: str | None = None) -> bool:
        """
        Checks if a key exists in a bucket

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: True if the key exists and False if not.
        :rtype: bool
        """
        obj = self.head_object(key, bucket_name)
        return obj is not None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_key(self, key: str, bucket_name: str | None = None) -> S3Transfer:
        """
        Returns a boto3.s3.Object

        :param key: the path to the key
        :param bucket_name: the name of the bucket
        :return: the key object from the bucket
        :rtype: boto3.s3.Object
        """
        s3_resource = self.get_session().resource(
            "s3",
            endpoint_url=self.conn_config.endpoint_url,
            config=self.config,
            verify=self.verify,
        )
        obj = s3_resource.Object(bucket_name, key)
        obj.load()
        return obj

    @provide_bucket_name
    @unify_bucket_name_and_key
    def read_key(self, key: str, bucket_name: str | None = None) -> str:
        """
        Reads a key from S3

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: the content of the key
        :rtype: str
        """
        obj = self.get_key(key, bucket_name)
        return obj.get()['Body'].read().decode('utf-8')

    @provide_bucket_name
    @unify_bucket_name_and_key
    def select_key(
        self,
        key: str,
        bucket_name: str | None = None,
        expression: str | None = None,
        expression_type: str | None = None,
        input_serialization: dict[str, Any] | None = None,
        output_serialization: dict[str, Any] | None = None,
    ) -> str:
        """
        Reads a key with S3 Select.

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :param expression: S3 Select expression
        :param expression_type: S3 Select expression type
        :param input_serialization: S3 Select input data serialization format
        :param output_serialization: S3 Select output data serialization format
        :return: retrieved subset of original data by S3 Select
        :rtype: str

        .. seealso::
            For more details about S3 Select parameters:
            http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.select_object_content
        """
        expression = expression or 'SELECT * FROM S3Object'
        expression_type = expression_type or 'SQL'

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
            OutputSerialization=output_serialization,
        )

        return b''.join(
            event['Records']['Payload'] for event in response['Payload'] if 'Records' in event
        ).decode('utf-8')

    @provide_bucket_name
    @unify_bucket_name_and_key
    def check_for_wildcard_key(
        self, wildcard_key: str, bucket_name: str | None = None, delimiter: str = ''
    ) -> bool:
        """
        Checks that a key matching a wildcard expression exists in a bucket

        :param wildcard_key: the path to the key
        :param bucket_name: the name of the bucket
        :param delimiter: the delimiter marks key hierarchy
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        return (
            self.get_wildcard_key(wildcard_key=wildcard_key, bucket_name=bucket_name, delimiter=delimiter)
            is not None
        )

    @provide_bucket_name
    @unify_bucket_name_and_key
    def get_wildcard_key(
        self, wildcard_key: str, bucket_name: str | None = None, delimiter: str = ''
    ) -> S3Transfer:
        """
        Returns a boto3.s3.Object object matching the wildcard expression

        :param wildcard_key: the path to the key
        :param bucket_name: the name of the bucket
        :param delimiter: the delimiter marks key hierarchy
        :return: the key object from the bucket or None if none has been found.
        :rtype: boto3.s3.Object
        """
        prefix = re.split(r'[\[\*\?]', wildcard_key, 1)[0]
        key_list = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        key_matches = [k for k in key_list if fnmatch.fnmatch(k, wildcard_key)]
        if key_matches:
            return self.get_key(key_matches[0], bucket_name)
        return None

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file(
        self,
        filename: Path | str,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        gzip: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        """
        Loads a local file to S3

        :param filename: path to the file to load.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param gzip: If True, the file will be compressed locally
        :param acl_policy: String specifying the canned ACL policy for the file being
            uploaded to the S3 bucket.
        """
        filename = str(filename)
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if gzip:
            with open(filename, 'rb') as f_in:
                filename_gz = f'{f_in.name}.gz'
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz
        if acl_policy:
            extra_args['ACL'] = acl_policy

        client = self.get_conn()
        client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args, Config=self.transfer_config)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_string(
        self,
        string_data: str,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        encoding: str | None = None,
        acl_policy: str | None = None,
        compression: str | None = None,
    ) -> None:
        """
        Loads a string to S3

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        :param string_data: str to set as content for the key.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param encoding: The string to byte encoding
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        :param compression: Type of compression to use, currently only gzip is supported.
        """
        encoding = encoding or 'utf-8'

        bytes_data = string_data.encode(encoding)

        # Compress string
        available_compressions = ['gzip']
        if compression is not None and compression not in available_compressions:
            raise NotImplementedError(
                f"Received {compression} compression type. "
                f"String can currently be compressed in {available_compressions} only."
            )
        if compression == 'gzip':
            bytes_data = gz.compress(bytes_data)

        file_obj = io.BytesIO(bytes_data)

        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_bytes(
        self,
        bytes_data: bytes,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        """
        Loads bytes to S3

        This is provided as a convenience to drop bytes data into S3. It uses the
        boto infrastructure to ship a file to s3.

        :param bytes_data: bytes to set as content for the key.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag to decide whether or not to overwrite the key
            if it already exists
        :param encrypt: If True, the file will be encrypted on the server-side
            by S3 and will be stored in an encrypted form while at rest in S3.
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        """
        file_obj = io.BytesIO(bytes_data)
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)
        file_obj.close()

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_file_obj(
        self,
        file_obj: BytesIO,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        """
        Loads a file object to S3

        :param file_obj: The file-like object to set as the content for the S3 key.
        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which to store the file
        :param replace: A flag that indicates whether to overwrite the key
            if it already exists.
        :param encrypt: If True, S3 encrypts the file on the server,
            and the file is stored in encrypted form at rest in S3.
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be uploaded
        """
        self._upload_file_obj(file_obj, key, bucket_name, replace, encrypt, acl_policy)

    def _upload_file_obj(
        self,
        file_obj: BytesIO,
        key: str,
        bucket_name: str | None = None,
        replace: bool = False,
        encrypt: bool = False,
        acl_policy: str | None = None,
    ) -> None:
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"The key {key} already exists.")

        extra_args = self.extra_args
        if encrypt:
            extra_args['ServerSideEncryption'] = "AES256"
        if acl_policy:
            extra_args['ACL'] = acl_policy

        client = self.get_conn()
        client.upload_fileobj(
            file_obj,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )

    def copy_object(
        self,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        source_version_id: str | None = None,
        acl_policy: str | None = None,
    ) -> None:
        """
        Creates a copy of an object that is already stored in S3.

        Note: the S3 connection used here needs to have access to both
        source and destination bucket/key.

        :param source_bucket_key: The key of the source object.

            It can be either full s3:// style url or relative path from root level.

            When it's specified as a full s3:// url, please omit source_bucket_name.
        :param dest_bucket_key: The key of the object to copy to.

            The convention to specify `dest_bucket_key` is the same
            as `source_bucket_key`.
        :param source_bucket_name: Name of the S3 bucket where the source object is in.

            It should be omitted when `source_bucket_key` is provided as a full s3:// url.
        :param dest_bucket_name: Name of the S3 bucket to where the object is copied.

            It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
        :param source_version_id: Version ID of the source object (OPTIONAL)
        :param acl_policy: The string to specify the canned ACL policy for the
            object to be copied which is private by default.
        """
        acl_policy = acl_policy or 'private'

        dest_bucket_name, dest_bucket_key = self.get_s3_bucket_key(
            dest_bucket_name, dest_bucket_key, 'dest_bucket_name', 'dest_bucket_key'
        )

        source_bucket_name, source_bucket_key = self.get_s3_bucket_key(
            source_bucket_name, source_bucket_key, 'source_bucket_name', 'source_bucket_key'
        )

        copy_source = {'Bucket': source_bucket_name, 'Key': source_bucket_key, 'VersionId': source_version_id}
        response = self.get_conn().copy_object(
            Bucket=dest_bucket_name, Key=dest_bucket_key, CopySource=copy_source, ACL=acl_policy
        )
        return response

    @provide_bucket_name
    def delete_bucket(self, bucket_name: str, force_delete: bool = False) -> None:
        """
        To delete s3 bucket, delete all s3 bucket objects and then delete the bucket.

        :param bucket_name: Bucket name
        :param force_delete: Enable this to delete bucket even if not empty
        :return: None
        :rtype: None
        """
        if force_delete:
            bucket_keys = self.list_keys(bucket_name=bucket_name)
            if bucket_keys:
                self.delete_objects(bucket=bucket_name, keys=bucket_keys)
        self.conn.delete_bucket(Bucket=bucket_name)

    def delete_objects(self, bucket: str, keys: str | list) -> None:
        """
        Delete keys from the bucket.

        :param bucket: Name of the bucket in which you are going to delete object(s)
        :param keys: The key(s) to delete from S3 bucket.

            When ``keys`` is a string, it's supposed to be the key name of
            the single object to delete.

            When ``keys`` is a list, it's supposed to be the list of the
            keys to delete.
        """
        if isinstance(keys, str):
            keys = [keys]

        s3 = self.get_conn()

        # We can only send a maximum of 1000 keys per request.
        # For details see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        for chunk in chunks(keys, chunk_size=1000):
            response = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
            deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
            self.log.info("Deleted: %s", deleted_keys)
            if "Errors" in response:
                errors_keys = [x['Key'] for x in response.get("Errors", [])]
                raise AirflowException(f"Errors when deleting: {errors_keys}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def download_file(self, key: str, bucket_name: str | None = None, local_path: str | None = None) -> str:
        """
        Downloads a file from the S3 location to the local file system.

        :param key: The key path in S3.
        :param bucket_name: The specific bucket to use.
        :param local_path: The local path to the downloaded file. If no path is provided it will use the
            system's temporary directory.
        :return: the file name.
        :rtype: str
        """
        self.log.info('Downloading source S3 file from Bucket %s with path %s', bucket_name, key)

        try:
            s3_obj = self.get_key(key, bucket_name)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 404:
                raise AirflowException(
                    f'The source file in Bucket {bucket_name} with path {key} does not exist'
                )
            else:
                raise e

        with NamedTemporaryFile(dir=local_path, prefix='airflow_tmp_', delete=False) as local_tmp_file:
            s3_obj.download_fileobj(
                local_tmp_file,
                ExtraArgs=self.extra_args,
                Config=self.transfer_config,
            )

        return local_tmp_file.name

    def generate_presigned_url(
        self,
        client_method: str,
        params: dict | None = None,
        expires_in: int = 3600,
        http_method: str | None = None,
    ) -> str | None:
        """
        Generate a presigned url given a client, its method, and arguments

        :param client_method: The client method to presign for.
        :param params: The parameters normally passed to ClientMethod.
        :param expires_in: The number of seconds the presigned url is valid for.
            By default it expires in an hour (3600 seconds).
        :param http_method: The http method to use on the generated url.
            By default, the http method is whatever is used in the method's model.
        :return: The presigned url.
        :rtype: str
        """
        s3_client = self.get_conn()
        try:
            return s3_client.generate_presigned_url(
                ClientMethod=client_method, Params=params, ExpiresIn=expires_in, HttpMethod=http_method
            )

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            return None

    @provide_bucket_name
    def get_bucket_tagging(self, bucket_name: str | None = None) -> list[dict[str, str]] | None:
        """
        Gets a List of tags from a bucket.

        :param bucket_name: The name of the bucket.
        :return: A List containing the key/value pairs for the tags
        :rtype: Optional[List[Dict[str, str]]]
        """
        try:
            s3_client = self.get_conn()
            result = s3_client.get_bucket_tagging(Bucket=bucket_name)['TagSet']
            self.log.info("S3 Bucket Tag Info: %s", result)
            return result
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def put_bucket_tagging(
        self,
        tag_set: list[dict[str, str]] | None = None,
        key: str | None = None,
        value: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Overwrites the existing TagSet with provided tags.  Must provide either a TagSet or a key/value pair.

        :param tag_set: A List containing the key/value pairs for the tags.
        :param key: The Key for the new TagSet entry.
        :param value: The Value for the new TagSet entry.
        :param bucket_name: The name of the bucket.
        :return: None
        :rtype: None
        """
        self.log.info("S3 Bucket Tag Info:\tKey: %s\tValue: %s\tSet: %s", key, value, tag_set)
        if not tag_set:
            tag_set = []
        if key and value:
            tag_set.append({'Key': key, 'Value': value})
        elif not tag_set or (key or value):
            message = 'put_bucket_tagging() requires either a predefined TagSet or a key/value pair.'
            self.log.error(message)
            raise ValueError(message)

        try:
            s3_client = self.get_conn()
            s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={'TagSet': tag_set})
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def delete_bucket_tagging(self, bucket_name: str | None = None) -> None:
        """
        Deletes all tags from a bucket.

        :param bucket_name: The name of the bucket.
        :return: None
        :rtype: None
        """
        s3_client = self.get_conn()
        s3_client.delete_bucket_tagging(Bucket=bucket_name)
