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
from functools import wraps
from inspect import signature
from typing import TYPE_CHECKING, Callable, Optional, TypeVar, cast
from urllib.parse import urlparse

import oss2
from oss2.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

T = TypeVar("T", bound=Callable)


def provide_bucket_name(func: T) -> T:
    """
    Function decorator that unifies bucket name and key taken from the key
    in case no bucket name and at least a key has been passed to the function.
    """
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        self = args[0]
        if 'bucket_name' not in bound_args.arguments or bound_args.arguments['bucket_name'] is None:
            if self.oss_conn_id:
                connection = self.get_connection(self.oss_conn_id)
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

        def get_key() -> str:
            if 'key' in bound_args.arguments:
                return 'key'
            raise ValueError('Missing key parameter!')

        key_name = get_key()
        if 'bucket_name' not in bound_args.arguments or bound_args.arguments['bucket_name'] is None:
            bound_args.arguments['bucket_name'], bound_args.arguments['key'] = OSSHook.parse_oss_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class OSSHook(BaseHook):
    """Interact with Alibaba Cloud OSS, using the oss2 library."""

    conn_name_attr = 'alibabacloud_conn_id'
    default_conn_name = 'oss_default'
    conn_type = 'oss'
    hook_name = 'OSS'

    def __init__(self, region, oss_conn_id='oss_default', *args, **kwargs) -> None:
        self.oss_conn_id = oss_conn_id
        self.oss_conn = self.get_connection(oss_conn_id)
        self.region = region
        super().__init__(*args, **kwargs)

    def get_conn(self) -> "Connection":
        """Returns connection for the hook."""
        return self.oss_conn

    @staticmethod
    def parse_oss_url(ossurl: str) -> tuple:
        """
        Parses the OSS Url into a bucket name and key.

        :param ossurl: The OSS Url to parse.
        :return: the parsed bucket name and key
        """
        parsed_url = urlparse(ossurl)

        if not parsed_url.netloc:
            raise AirflowException(f'Please provide a bucket_name instead of "{ossurl}"')

        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        return bucket_name, key

    @unify_bucket_name_and_key
    def object_exists(self, key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Check if object exists.

        :param key: the path of the object
        :param bucket_name: the name of the bucket
        :return: True if it exists and False if not.
        :rtype: bool
        """
        try:
            return self.get_bucket(bucket_name).object_exists(key)
        except ClientError as e:
            self.log.error(e.message)
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name: Optional[str] = None) -> oss2.api.Bucket:
        """
        Returns a oss2.Bucket object

        :param bucket_name: the name of the bucket
        :return: the bucket object to the bucket name.
        :rtype: oss2.api.Bucket
        """
        auth = self.get_credential()
        return oss2.Bucket(auth, 'http://oss-' + self.region + '.aliyuncs.com', bucket_name)

    @unify_bucket_name_and_key
    def load_string(self, key: str, content: str, bucket_name: Optional[str] = None) -> None:
        """
        Loads a string to OSS

        :param key: the path of the object
        :param content: str to set as content for the key.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).put_object(key, content)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    @unify_bucket_name_and_key
    def upload_local_file(
        self,
        key: str,
        file: str,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Upload a local file to OSS

        :param key: the OSS path of the object
        :param file: local file to upload.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).put_object_from_file(key, file)
        except Exception as e:
            raise AirflowException(f"Errors when upload file: {e}")

    @unify_bucket_name_and_key
    def download_file(
        self,
        key: str,
        local_file: str,
        bucket_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Download file from OSS

        :param key: key of the file-like object to download.
        :param local_file: local path + file name to save.
        :param bucket_name: the name of the bucket
        :return: the file name.
        :rtype: str
        """
        try:
            self.get_bucket(bucket_name).get_object_to_file(key, local_file)
        except Exception as e:
            self.log.error(e)
            return None
        return local_file

    @unify_bucket_name_and_key
    def delete_object(
        self,
        key: str,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Delete object from OSS

        :param key: key of the object to delete.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).delete_object(key)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {key}")

    @unify_bucket_name_and_key
    def delete_objects(
        self,
        key: list,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Delete objects from OSS

        :param key: keys list of the objects to delete.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).batch_delete_objects(key)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {key}")

    @provide_bucket_name
    def delete_bucket(
        self,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Delete bucket from OSS

        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).delete_bucket()
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {bucket_name}")

    @provide_bucket_name
    def create_bucket(
        self,
        bucket_name: Optional[str] = None,
    ) -> None:
        """
        Create bucket

        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).create_bucket()
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when create bucket: {bucket_name}")

    def get_credential(self) -> oss2.auth.Auth:
        extra_config = self.oss_conn.extra_dejson
        auth_type = extra_config.get('auth_type', None)
        if not auth_type:
            raise Exception("No auth_type specified in extra_config. ")

        if auth_type == 'AK':
            oss_access_key_id = extra_config.get('access_key_id', None)
            oss_access_key_secret = extra_config.get('access_key_secret', None)
            if not oss_access_key_id:
                raise Exception("No access_key_id is specified for connection: " + self.oss_conn_id)
            if not oss_access_key_secret:
                raise Exception("No access_key_secret is specified for connection: " + self.oss_conn_id)
            return oss2.Auth(oss_access_key_id, oss_access_key_secret)
        else:
            raise Exception("Unsupported auth_type: " + auth_type)
