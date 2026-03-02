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

from collections.abc import Callable
from functools import wraps
from inspect import signature
from typing import TYPE_CHECKING, TypeVar, cast
from urllib.parse import urlsplit

import oss2
from oss2.exceptions import ClientError

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection

T = TypeVar("T", bound=Callable)


def provide_bucket_name(func: T) -> T:
    """Unify bucket name and key if a key is provided but not a bucket name."""
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)
        self = args[0]
        if bound_args.arguments.get("bucket_name") is None and self.oss_conn_id:
            connection = self.get_connection(self.oss_conn_id)
            if connection.schema:
                bound_args.arguments["bucket_name"] = connection.schema

        return func(*bound_args.args, **bound_args.kwargs)

    return cast("T", wrapper)


def unify_bucket_name_and_key(func: T) -> T:
    """Unify bucket name and key if a key is provided but not a bucket name."""
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> T:
        bound_args = function_signature.bind(*args, **kwargs)

        def get_key() -> str:
            if "key" in bound_args.arguments:
                return "key"
            raise ValueError("Missing key parameter!")

        key_name = get_key()
        if bound_args.arguments.get("bucket_name") is None:
            bound_args.arguments["bucket_name"], bound_args.arguments["key"] = OSSHook.parse_oss_url(
                bound_args.arguments[key_name]
            )

        return func(*bound_args.args, **bound_args.kwargs)

    return cast("T", wrapper)


class OSSHook(BaseHook):
    """Interact with Alibaba Cloud OSS, using the oss2 library."""

    conn_name_attr = "oss_conn_id"
    default_conn_name = "oss_default"
    conn_type = "oss"
    hook_name = "OSS"

    def __init__(self, region: str | None = None, oss_conn_id="oss_default", *args, **kwargs) -> None:
        self.oss_conn_id = oss_conn_id
        self.oss_conn = self.get_connection(oss_conn_id)
        self.region = region or self.get_default_region()
        super().__init__(*args, **kwargs)

    def get_conn(self) -> Connection:
        """Return connection for the hook."""
        return self.oss_conn

    @staticmethod
    def parse_oss_url(ossurl: str) -> tuple:
        """
        Parse the OSS Url into a bucket name and key.

        :param ossurl: The OSS Url to parse.
        :return: the parsed bucket name and key
        """
        parsed_url = urlsplit(ossurl)

        if not parsed_url.netloc:
            raise AirflowException(f'Please provide a bucket_name instead of "{ossurl}"')

        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip("/")

        return bucket_name, key

    @provide_bucket_name
    @unify_bucket_name_and_key
    def object_exists(self, key: str, bucket_name: str | None = None) -> bool:
        """
        Check if object exists.

        :param key: the path of the object
        :param bucket_name: the name of the bucket
        :return: True if it exists and False if not.
        """
        try:
            return self.get_bucket(bucket_name).object_exists(key)
        except ClientError as e:
            self.log.error(e.message)
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name: str | None = None) -> oss2.api.Bucket:
        """
        Return a oss2.Bucket object.

        :param bucket_name: the name of the bucket
        :return: the bucket object to the bucket name.
        """
        auth = self.get_credential()
        return oss2.Bucket(auth, f"https://oss-{self.region}.aliyuncs.com", bucket_name)

    @provide_bucket_name
    @unify_bucket_name_and_key
    def load_string(self, key: str, content: str, bucket_name: str | None = None) -> None:
        """
        Load a string to OSS.

        :param key: the path of the object
        :param content: str to set as content for the key.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).put_object(key, content)
        except Exception as e:
            raise AirflowException(f"Errors: {e}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def upload_local_file(
        self,
        key: str,
        file: str,
        bucket_name: str | None = None,
    ) -> None:
        """
        Upload a local file to OSS.

        :param key: the OSS path of the object
        :param file: local file to upload.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).put_object_from_file(key, file)
        except Exception as e:
            raise AirflowException(f"Errors when upload file: {e}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def download_file(
        self,
        key: str,
        local_file: str,
        bucket_name: str | None = None,
    ) -> str | None:
        """
        Download file from OSS.

        :param key: key of the file-like object to download.
        :param local_file: local path + file name to save.
        :param bucket_name: the name of the bucket
        :return: the file name.
        """
        try:
            self.get_bucket(bucket_name).get_object_to_file(key, local_file)
        except Exception as e:
            self.log.error(e)
            return None
        return local_file

    @provide_bucket_name
    @unify_bucket_name_and_key
    def delete_object(
        self,
        key: str,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete object from OSS.

        :param key: key of the object to delete.
        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).delete_object(key)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when deleting: {key}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def delete_objects(
        self,
        key: list,
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete objects from OSS.

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
        bucket_name: str | None = None,
    ) -> None:
        """
        Delete bucket from OSS.

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
        bucket_name: str | None = None,
    ) -> None:
        """
        Create bucket.

        :param bucket_name: the name of the bucket
        """
        try:
            self.get_bucket(bucket_name).create_bucket()
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when create bucket: {bucket_name}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def append_string(self, bucket_name: str | None, content: str, key: str, pos: int) -> None:
        """
        Append string to a remote existing file.

        :param bucket_name: the name of the bucket
        :param content: content to be appended
        :param key: oss bucket key
        :param pos: position of the existing file where the content will be appended
        """
        self.log.info("Write oss bucket. key: %s, pos: %s", key, pos)
        try:
            self.get_bucket(bucket_name).append_object(key, pos, content)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when append string for object: {key}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def read_key(self, bucket_name: str | None, key: str) -> str:
        """
        Read oss remote object content with the specified key.

        :param bucket_name: the name of the bucket
        :param key: oss bucket key
        """
        self.log.info("Read oss key: %s", key)
        try:
            return self.get_bucket(bucket_name).get_object(key).read().decode("utf-8")
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when read bucket object: {key}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def head_key(self, bucket_name: str | None, key: str) -> oss2.models.HeadObjectResult:
        """
        Get meta info of the specified remote object.

        :param bucket_name: the name of the bucket
        :param key: oss bucket key
        """
        self.log.info("Head Object oss key: %s", key)
        try:
            return self.get_bucket(bucket_name).head_object(key)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when head bucket object: {key}")

    @provide_bucket_name
    @unify_bucket_name_and_key
    def key_exist(self, bucket_name: str | None, key: str) -> bool:
        """
        Find out whether the specified key exists in the oss remote storage.

        :param bucket_name: the name of the bucket
        :param key: oss bucket key
        """
        # full_path = None
        self.log.info("Looking up oss bucket %s for bucket key %s ...", bucket_name, key)
        try:
            return self.get_bucket(bucket_name).object_exists(key)
        except Exception as e:
            self.log.error(e)
            raise AirflowException(f"Errors when check bucket object existence: {key}")

    def get_credential(self) -> oss2.auth.Auth:
        extra_config = self.oss_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")
        oss_access_key_id = extra_config.get("access_key_id", None)
        oss_access_key_secret = extra_config.get("access_key_secret", None)
        if not oss_access_key_id:
            raise ValueError(f"No access_key_id is specified for connection: {self.oss_conn_id}")

        if not oss_access_key_secret:
            raise ValueError(f"No access_key_secret is specified for connection: {self.oss_conn_id}")

        return oss2.Auth(oss_access_key_id, oss_access_key_secret)

    def get_default_region(self) -> str:
        extra_config = self.oss_conn.extra_dejson
        auth_type = extra_config.get("auth_type", None)
        if not auth_type:
            raise ValueError("No auth_type specified in extra_config. ")

        if auth_type != "AK":
            raise ValueError(f"Unsupported auth_type: {auth_type}")

        default_region = extra_config.get("region", None)
        if not default_region:
            raise ValueError(f"No region is specified for connection: {self.oss_conn_id}")
        return default_region
