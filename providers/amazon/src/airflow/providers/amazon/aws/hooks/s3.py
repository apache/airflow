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

import asyncio
import fnmatch
import gzip as gz
import hashlib
import inspect
import logging
import os
import re
import shutil
import time
import warnings
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import suppress
from copy import deepcopy
from datetime import datetime
from functools import cached_property, wraps
from inspect import signature
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile, gettempdir
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

if TYPE_CHECKING:
    from aiobotocore.client import AioBaseClient
    from mypy_boto3_s3.service_resource import (
        Bucket as S3Bucket,
        Object as S3ResourceObject,
    )

    from airflow.providers.amazon.version_compat import ArgNotSet


from asgiref.sync import sync_to_async
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.exceptions import S3HookUriParseFailure
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.tags import format_tags
from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException
from airflow.utils.helpers import chunks

logger = logging.getLogger(__name__)


# Explicit value that would remove ACLs from a copy
# No conflicts with Canned ACLs:
#   https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
NO_ACL = "no-acl"


def provide_bucket_name(func: Callable) -> Callable:
    """Provide a bucket name taken from the connection if no bucket name has been passed to the function."""
    if hasattr(func, "_unify_bucket_name_and_key_wrapped"):
        logger.warning("`unify_bucket_name_and_key` should wrap `provide_bucket_name`.")

    function_signature = signature(func)
    if "bucket_name" not in function_signature.parameters:
        raise RuntimeError(
            "Decorator provide_bucket_name should only wrap a function with param 'bucket_name'."
        )

    async def maybe_add_bucket_name(*args, **kwargs):
        bound_args = function_signature.bind(*args, **kwargs)

        if not bound_args.arguments.get("bucket_name"):
            self = args[0]
            if self.aws_conn_id:
                connection = await sync_to_async(self.get_connection)(self.aws_conn_id)
                if connection.schema:
                    bound_args.arguments["bucket_name"] = connection.schema
        return bound_args

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound_args = await maybe_add_bucket_name(*args, **kwargs)
            print(f"invoking async function {func=}")
            return await func(*bound_args.args, **bound_args.kwargs)

    elif inspect.isasyncgenfunction(func):

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound_args = await maybe_add_bucket_name(*args, **kwargs)
            async for thing in func(*bound_args.args, **bound_args.kwargs):
                yield thing

    else:

        @wraps(func)
        def wrapper(*args, **kwargs) -> Callable:
            bound_args = function_signature.bind(*args, **kwargs)

            if not bound_args.arguments.get("bucket_name"):
                self = args[0]

                if "bucket_name" in self.service_config:
                    bound_args.arguments["bucket_name"] = self.service_config["bucket_name"]

            return func(*bound_args.args, **bound_args.kwargs)

    return wrapper


def unify_bucket_name_and_key(func: Callable) -> Callable:
    """Unify bucket name and key in case no bucket name and at least a key has been passed to the function."""
    function_signature = signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = function_signature.bind(*args, **kwargs)

        if "wildcard_key" in bound_args.arguments:
            key_name = "wildcard_key"
        elif "key" in bound_args.arguments:
            key_name = "key"
        else:
            raise ValueError("Missing key parameter!")

        if "bucket_name" not in bound_args.arguments:
            with suppress(S3HookUriParseFailure):
                (
                    bound_args.arguments["bucket_name"],
                    bound_args.arguments[key_name],
                ) = S3Hook.parse_s3_url(bound_args.arguments[key_name])

        return func(*bound_args.args, **bound_args.kwargs)

    # set attr _unify_bucket_name_and_key_wrapped so that we can check at
    # class definition that unify is the first decorator applied
    # if provide_bucket_name is applied first, and there's a bucket defined in conn
    # then if user supplies full key, bucket in key is not respected
    wrapper._unify_bucket_name_and_key_wrapped = True  # type: ignore[attr-defined]
    return wrapper


class S3Hook(AwsBaseHook):
    """
    Interact with Amazon Simple Storage Service (S3).

    Provide thick wrapper around :external+boto3:py:class:`boto3.client("s3") <S3.Client>`
    and :external+boto3:py:class:`boto3.resource("s3") <S3.ServiceResource>`.

    :param transfer_config_args: Configuration object for managed S3 transfers.
    :param extra_args: Extra arguments that may be passed to the download/upload operations.

    .. seealso::
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#s3-transfers

        - For allowed upload extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS``.
        - For allowed download extra arguments see ``boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS``.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(
        self,
        aws_conn_id: str | None | ArgNotSet = AwsBaseHook.default_conn_name,
        transfer_config_args: dict | None = None,
        extra_args: dict | None = None,
        *args,
        **kwargs,
    ) -> None:
        kwargs["client_type"] = "s3"
        kwargs["aws_conn_id"] = aws_conn_id
        self._requester_pays = kwargs.pop("requester_pays", False)

        if transfer_config_args and not isinstance(transfer_config_args, dict):
            raise TypeError(f"transfer_config_args expected dict, got {type(transfer_config_args).__name__}.")
        self.transfer_config = TransferConfig(**transfer_config_args or {})

        if extra_args and not isinstance(extra_args, dict):
            raise TypeError(f"extra_args expected dict, got {type(extra_args).__name__}.")
        self._extra_args = extra_args or {}

        super().__init__(*args, **kwargs)

    @cached_property
    def resource(self):
        return self.get_session().resource(
            self.service_name,
            endpoint_url=self.conn_config.get_service_endpoint_url(service_name=self.service_name),
            config=self.config,
            verify=self.verify,
        )

    @property
    def extra_args(self):
        """Return hook's extra arguments (immutable)."""
        return deepcopy(self._extra_args)

    @staticmethod
    def parse_s3_url(s3url: str) -> tuple[str, str]:
        """
        Parse the S3 Url into a bucket name and key.

        See https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html
        for valid url formats.

        :param s3url: The S3 Url to parse.
        :return: the parsed bucket name and key
        """
        valid_s3_format = "S3://bucket-name/key-name"
        valid_s3_virtual_hosted_format = "https://bucket-name.s3.region-code.amazonaws.com/key-name"
        format = s3url.split("//")
        if re.match(r"s3[na]?:", format[0], re.IGNORECASE):
            parsed_url = urlsplit(s3url, allow_fragments=False)
            if not parsed_url.netloc:
                raise S3HookUriParseFailure(
                    "Please provide a bucket name using a valid format of the form: "
                    f'{valid_s3_format} or {valid_s3_virtual_hosted_format} but provided: "{s3url}"'
                )

            bucket_name = parsed_url.netloc
            key = parsed_url.path.lstrip("/")
        elif format[0] == "https:":
            temp_split = format[1].split(".")
            if temp_split[0] == "s3":
                # "https://s3.region-code.amazonaws.com/bucket-name/key-name"
                _, bucket_name, key = format[1].split("/", 2)
            elif temp_split[1] == "s3":
                # "https://bucket-name.s3.region-code.amazonaws.com/key-name"
                bucket_name = temp_split[0]
                key = format[1].partition("/")[-1]
            else:
                raise S3HookUriParseFailure(
                    "Please provide a bucket name using a valid virtually hosted format which should "
                    f'be of the form: {valid_s3_virtual_hosted_format} but provided: "{s3url}"'
                )
        else:
            raise S3HookUriParseFailure(
                "Please provide a bucket name using a valid format of the form: "
                f'{valid_s3_format} or {valid_s3_virtual_hosted_format} but provided: "{s3url}"'
            )
        return bucket_name, key

    @staticmethod
    def get_s3_bucket_key(
        bucket: str | None, key: str, bucket_param_name: str, key_param_name: str
    ) -> tuple[str, str]:
        """
        Get the S3 bucket name and key.

        From either:
        - bucket name and key. Return the info as it is after checking `key` is a relative path.
        - key. Must be a full s3:// url.

        :param bucket: The S3 bucket name
        :param key: The S3 key
        :param bucket_param_name: The parameter name containing the bucket name
        :param key_param_name: The parameter name containing the key name
        :return: the parsed bucket name and key
        """
        if bucket is None:
            return S3Hook.parse_s3_url(key)

        parsed_url = urlsplit(key)
        if parsed_url.scheme != "" or parsed_url.netloc != "":
            raise TypeError(
                f"If `{bucket_param_name}` is provided, {key_param_name} should be a relative path "
                "from root level, rather than a full s3:// url"
            )
        return bucket, key

    @provide_bucket_name
    def check_for_bucket(self, bucket_name: str | None = None) -> bool:
        """
        Check if bucket_name exists.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.head_bucket`

        :param bucket_name: the name of the bucket
        :return: True if it exists and False if not.
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
            return_code = int(e.response["Error"]["Code"])
            if return_code == 404:
                self.log.info('Bucket "%s" does not exist', bucket_name)
            elif return_code == 403:
                self.log.error(
                    'Access to bucket "%s" is forbidden or there was an error with the request',
                    bucket_name,
                )
                self.log.error(e)
            return False

    @provide_bucket_name
    def get_bucket(self, bucket_name: str | None = None) -> S3Bucket:
        """
        Return a :py:class:`S3.Bucket` object.

        .. seealso::
            - :external+boto3:py:meth:`S3.ServiceResource.Bucket`

        :param bucket_name: the name of the bucket
        :return: the bucket object to the bucket name.
        """
        return self.resource.Bucket(bucket_name)

    @provide_bucket_name
    def create_bucket(self, bucket_name: str | None = None, region_name: str | None = None) -> None:
        """
        Create an Amazon S3 bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.create_bucket`

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

        if region_name == "us-east-1":
            self.get_conn().create_bucket(Bucket=bucket_name)
        else:
            self.get_conn().create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )

    @provide_bucket_name
    def check_for_prefix(self, prefix: str, delimiter: str, bucket_name: str | None = None) -> bool:
        """
        Check that a prefix exists in a bucket.

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :return: False if the prefix does not exist in the bucket and True if it does.
        """
        if not prefix.endswith(delimiter):
            prefix += delimiter
        prefix_split = re.split(rf"(\w+[{delimiter}])$", prefix, 1)
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
        List prefixes in a bucket under prefix.

        .. seealso::
            - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched prefixes
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
            "PaginationConfig": config,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)

        prefixes: list[str] = []
        for page in response:
            if "CommonPrefixes" in page:
                prefixes.extend(common_prefix["Prefix"] for common_prefix in page["CommonPrefixes"])

        return prefixes

    @unify_bucket_name_and_key
    @provide_bucket_name
    async def get_head_object_async(
        self, client: AioBaseClient, key: str, bucket_name: str | None = None
    ) -> dict[str, Any] | None:
        """
        Retrieve metadata of an object.

        :param client: aiobotocore client
        :param bucket_name: Name of the bucket in which the file is stored
        :param key: S3 key that will point to the file
        """
        head_object_val: dict[str, Any] | None = None
        try:
            params = {
                "Bucket": bucket_name,
                "Key": key,
            }
            if self._requester_pays:
                params["RequestPayer"] = "requester"
            head_object_val = await client.head_object(**params)
            return head_object_val
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return head_object_val
            raise e

    async def list_prefixes_async(
        self,
        client: AioBaseClient,
        bucket_name: str | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> list[Any]:
        """
        List prefixes in a bucket under prefix.

        :param client: ClientCreatorContext
        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched prefixes
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = client.get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
            "PaginationConfig": config,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)

        prefixes = []
        async for page in response:
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    prefixes.append(common_prefix["Prefix"])

        return prefixes

    @provide_bucket_name
    async def get_file_metadata_async(
        self, client: AioBaseClient, bucket_name: str, key: str | None = None
    ) -> AsyncIterator[Any]:
        """
        Get a list of files that a key matching a wildcard expression exists in a bucket asynchronously.

        :param client: aiobotocore client
        :param bucket_name: the name of the bucket
        :param key: the path to the key
        """
        prefix = re.split(r"[\[\*\?]", key, 1)[0] if key else ""
        delimiter = ""
        paginator = client.get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)
        async for page in response:
            if "Contents" in page:
                for row in page["Contents"]:
                    yield row

    async def _check_key_async(
        self,
        client: AioBaseClient,
        bucket_val: str,
        wildcard_match: bool,
        key: str,
        use_regex: bool = False,
    ) -> bool:
        """
        Get a list of files that a key matching a wildcard expression or get the head object.

        If wildcard_match is True get list of files that a key matching a wildcard
        expression exists in a bucket asynchronously and return the boolean value. If wildcard_match
        is False get the head object from the bucket and return the boolean value.

        :param client: aiobotocore client
        :param bucket_val: the name of the bucket
        :param key: S3 keys that will point to the file
        :param wildcard_match: the path to the key
        :param use_regex: whether to use regex to check bucket
        """
        bucket_name, key = self.get_s3_bucket_key(bucket_val, key, "bucket_name", "bucket_key")
        if wildcard_match:
            async for k in self.get_file_metadata_async(client, bucket_name, key):
                if fnmatch.fnmatch(k["Key"], key):
                    return True
            return False
        if use_regex:
            async for k in self.get_file_metadata_async(client, bucket_name):
                if re.match(pattern=key, string=k["Key"]):
                    return True
            return False
        return bool(await self.get_head_object_async(client, key, bucket_name))

    async def check_key_async(
        self,
        client: AioBaseClient,
        bucket: str,
        bucket_keys: str | list[str],
        wildcard_match: bool,
        use_regex: bool = False,
    ) -> bool:
        """
        Get a list of files that a key matching a wildcard expression or get the head object.

        If wildcard_match is True get list of files that a key matching a wildcard
        expression exists in a bucket asynchronously and return the boolean value. If wildcard_match
        is False get the head object from the bucket and return the boolean value.

        :param client: aiobotocore client
        :param bucket: the name of the bucket
        :param bucket_keys: S3 keys that will point to the file
        :param wildcard_match: the path to the key
        :param use_regex: whether to use regex to check bucket
        """
        if isinstance(bucket_keys, list):
            return all(
                await asyncio.gather(
                    *(
                        self._check_key_async(client, bucket, wildcard_match, key, use_regex)
                        for key in bucket_keys
                    )
                )
            )
        return await self._check_key_async(client, bucket, wildcard_match, bucket_keys, use_regex)

    async def check_for_prefix_async(
        self,
        client: AioBaseClient,
        prefix: str,
        delimiter: str,
        bucket_name: str | None = None,
    ) -> bool:
        """
        Check that a prefix exists in a bucket.

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :return: False if the prefix does not exist in the bucket and True if it does.
        """
        if not prefix.endswith(delimiter):
            prefix += delimiter
        prefix_split = re.split(rf"(\w+[{delimiter}])$", prefix, 1)
        previous_level = prefix_split[0]
        plist = await self.list_prefixes_async(client, bucket_name, previous_level, delimiter)
        return prefix in plist

    async def _check_for_prefix_async(
        self,
        client: AioBaseClient,
        prefix: str,
        delimiter: str,
        bucket_name: str | None = None,
    ) -> bool:
        return await self.check_for_prefix_async(
            client, prefix=prefix, delimiter=delimiter, bucket_name=bucket_name
        )

    async def get_files_async(
        self,
        client: AioBaseClient,
        bucket: str,
        bucket_keys: str | list[str],
        wildcard_match: bool,
        delimiter: str | None = "/",
    ) -> list[Any]:
        """Get a list of files in the bucket."""
        # Validate that bucket_keys is in fact a list, otherwise, the characters will be split
        if isinstance(bucket_keys, str):
            bucket_keys = [bucket_keys]

        keys: list[Any] = []
        for key in bucket_keys:
            prefix = key
            if wildcard_match:
                prefix = re.split(r"[\[*?]", key, 1)[0]

            paginator = client.get_paginator("list_objects_v2")
            params = {
                "Bucket": bucket,
                "Prefix": prefix,
                "Delimiter": delimiter,
            }
            if self._requester_pays:
                params["RequestPayer"] = "requester"
            response = paginator.paginate(**params)
            async for page in response:
                if "Contents" in page:
                    keys.extend(
                        k.get("Key") for k in page["Contents"] if isinstance(k.get("Size"), (int, float))
                    )
        return keys

    async def _list_keys_async(
        self,
        client: AioBaseClient,
        bucket_name: str | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> list[str]:
        """
        List keys in a bucket under prefix and not containing delimiter.

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched keys
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = client.get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "Delimiter": delimiter,
            "PaginationConfig": config,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)

        keys = []
        async for page in response:
            if "Contents" in page:
                for k in page["Contents"]:
                    keys.append(k["Key"])

        return keys

    def _list_key_object_filter(
        self,
        keys: list,
        from_datetime: datetime | None = None,
        to_datetime: datetime | None = None,
    ) -> list:
        def _is_in_period(input_date: datetime) -> bool:
            if from_datetime is not None and input_date <= from_datetime:
                return False
            if to_datetime is not None and input_date > to_datetime:
                return False
            return True

        return [k["Key"] for k in keys if _is_in_period(k["LastModified"])]

    async def is_keys_unchanged_async(
        self,
        client: AioBaseClient,
        bucket_name: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: set[str] | None = None,
        inactivity_seconds: int = 0,
        allow_delete: bool = True,
        last_activity_time: datetime | None = None,
    ) -> dict[str, Any]:
        """
        Check if new objects have been uploaded and the period has passed; update sensor state accordingly.

        :param client: aiobotocore client
        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param inactivity_period:  the total seconds of inactivity to designate
            keys unchanged. Note, this mechanism is not real time and
            this operator may not return until a poke_interval after this period
            has passed with no additional objects sensed.
        :param min_objects: the minimum number of objects needed for keys unchanged
            sensor to be considered valid.
        :param previous_objects: the set of object ids found during the last poke.
        :param inactivity_seconds: number of inactive seconds
        :param allow_delete: Should this sensor consider objects being deleted
            between pokes valid behavior. If true a warning message will be logged
            when this happens. If false an error will be raised.
        :param last_activity_time: last activity datetime.
        """
        if not previous_objects:
            previous_objects = set()
        list_keys = await self._list_keys_async(client=client, bucket_name=bucket_name, prefix=prefix)
        current_objects = set(list_keys)
        current_num_objects = len(current_objects)
        if current_num_objects > len(previous_objects):
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info(
                "New objects found at %s, resetting last_activity_time.",
                os.path.join(bucket_name, prefix),
            )
            self.log.debug("New objects: %s", current_objects - previous_objects)
            last_activity_time = datetime.now()
            inactivity_seconds = 0
            previous_objects = current_objects
            return {
                "status": "pending",
                "previous_objects": previous_objects,
                "last_activity_time": last_activity_time,
                "inactivity_seconds": inactivity_seconds,
            }

        if len(previous_objects) - len(current_objects):
            # During the last poke interval objects were deleted.
            if allow_delete:
                deleted_objects = previous_objects - current_objects
                previous_objects = current_objects
                last_activity_time = datetime.now()
                self.log.info(
                    "Objects were deleted during the last poke interval. Updating the "
                    "file counter and resetting last_activity_time:\n%s",
                    deleted_objects,
                )
                return {
                    "status": "pending",
                    "previous_objects": previous_objects,
                    "last_activity_time": last_activity_time,
                    "inactivity_seconds": inactivity_seconds,
                }

            return {
                "status": "error",
                "message": f"{os.path.join(bucket_name, prefix)} between pokes.",
            }

        if last_activity_time:
            inactivity_seconds = int(
                (datetime.now(last_activity_time.tzinfo) - last_activity_time).total_seconds()
            )
        else:
            # Handles the first poke where last inactivity time is None.
            last_activity_time = datetime.now()
            inactivity_seconds = 0

        if inactivity_seconds >= inactivity_period:
            path = os.path.join(bucket_name, prefix)

            if current_num_objects >= min_objects:
                success_message = (
                    f"SUCCESS: Sensor found {current_num_objects} objects at {path}. "
                    "Waited at least {inactivity_period} seconds, with no new objects uploaded."
                )
                self.log.info(success_message)
                return {
                    "status": "success",
                    "message": success_message,
                }

            self.log.error(
                "FAILURE: Inactivity Period passed, not enough objects found in %s",
                path,
            )
        return {
            "status": "pending",
            "previous_objects": previous_objects,
            "last_activity_time": last_activity_time,
            "inactivity_seconds": inactivity_seconds,
        }

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
        apply_wildcard: bool = False,
    ) -> list:
        """
        List keys in a bucket under prefix and not containing delimiter.

        .. seealso::
            - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

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
        :param apply_wildcard: whether to treat '*' as a wildcard or a plain symbol in the prefix.

        **Example**: Returns the list of S3 object with LastModified attr greater than from_datetime
             and less than to_datetime:

        .. code-block:: python

            def object_filter(
                keys: list,
                from_datetime: datetime | None = None,
                to_datetime: datetime | None = None,
            ) -> list:
                def _is_in_period(input_date: datetime) -> bool:
                    if from_datetime is not None and input_date < from_datetime:
                        return False

                    if to_datetime is not None and input_date > to_datetime:
                        return False
                    return True

                return [k["Key"] for k in keys if _is_in_period(k["LastModified"])]

        :return: a list of matched keys
        """
        _original_prefix = prefix or ""
        _apply_wildcard = bool(apply_wildcard and "*" in _original_prefix)
        _prefix = _original_prefix.split("*", 1)[0] if _apply_wildcard else _original_prefix
        delimiter = delimiter or ""
        start_after_key = start_after_key or ""
        object_filter_usr = object_filter
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": _prefix,
            "Delimiter": delimiter,
            "PaginationConfig": config,
            "StartAfter": start_after_key,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)

        keys: list[str] = []
        for page in response:
            if "Contents" in page:
                new_keys = page["Contents"]
                if _apply_wildcard:
                    new_keys = (k for k in new_keys if fnmatch.fnmatch(k["Key"], _original_prefix))
                keys.extend(new_keys)
        if object_filter_usr is not None:
            return object_filter_usr(keys, from_datetime, to_datetime)

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
        .. deprecated:: <9.13.0> Use `iter_file_metadata` instead.

        This method `get_file_metadata` is deprecated. Calling this method will result in all matching keys
        being loaded into a single list, and can often result in out-of-memory exceptions.
        """
        warnings.warn(
            "This method `get_file_metadata` is deprecated. Calling this method will result in all matching "
            "keys being loaded into a single list, and can often result in out-of-memory exceptions. "
            "Instead, use `iter_file_metadata`.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )

        return list(
            self.iter_file_metadata(
                prefix=prefix,
                bucket_name=bucket_name,
                page_size=page_size,
                max_items=max_items,
            )
        )

    @provide_bucket_name
    def iter_file_metadata(
        self,
        prefix: str,
        bucket_name: str | None = None,
        page_size: int | None = None,
        max_items: int | None = None,
    ) -> Iterator:
        """
        Yield metadata objects from a bucket under a prefix.

        .. seealso::
            - :external+boto3:py:class:`S3.Paginator.ListObjectsV2`

        :param prefix: a key prefix
        :param bucket_name: the name of the bucket
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: an Iterator of metadata of objects
        """
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }

        paginator = self.get_conn().get_paginator("list_objects_v2")
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "PaginationConfig": config,
        }
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        response = paginator.paginate(**params)

        for page in response:
            if "Contents" in page:
                yield from page["Contents"]

    @unify_bucket_name_and_key
    @provide_bucket_name
    def head_object(self, key: str, bucket_name: str | None = None) -> dict | None:
        """
        Retrieve metadata of an object.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.head_object`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: metadata of an object
        """
        try:
            params = {
                "Bucket": bucket_name,
                "Key": key,
            }
            if self._requester_pays:
                params["RequestPayer"] = "requester"
            return self.get_conn().head_object(**params)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise e

    @unify_bucket_name_and_key
    @provide_bucket_name
    def check_for_key(self, key: str, bucket_name: str | None = None) -> bool:
        """
        Check if a key exists in a bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.head_object`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: True if the key exists and False if not.
        """
        obj = self.head_object(key, bucket_name)
        return obj is not None

    @unify_bucket_name_and_key
    @provide_bucket_name
    def get_key(self, key: str, bucket_name: str | None = None) -> S3ResourceObject:
        """
        Return a :py:class:`S3.Object`.

        .. seealso::
            - :external+boto3:py:meth:`S3.ServiceResource.Object`

        :param key: the path to the key
        :param bucket_name: the name of the bucket
        :return: the key object from the bucket
        """

        def sanitize_extra_args() -> dict[str, str]:
            """Parse extra_args and return a dict with only the args listed in ALLOWED_DOWNLOAD_ARGS."""
            return {
                arg_name: arg_value
                for (arg_name, arg_value) in self.extra_args.items()
                if arg_name in S3Transfer.ALLOWED_DOWNLOAD_ARGS
            }

        params = sanitize_extra_args()
        if self._requester_pays:
            params["RequestPayer"] = "requester"
        obj = self.resource.Object(bucket_name, key)
        obj.load(**params)
        return obj

    @unify_bucket_name_and_key
    @provide_bucket_name
    def read_key(self, key: str, bucket_name: str | None = None) -> str:
        """
        Read a key from S3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Object.get`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :return: the content of the key
        """
        obj = self.get_key(key, bucket_name)
        return obj.get()["Body"].read().decode("utf-8")

    @unify_bucket_name_and_key
    @provide_bucket_name
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
        Read a key with S3 Select.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.select_object_content`

        :param key: S3 key that will point to the file
        :param bucket_name: Name of the bucket in which the file is stored
        :param expression: S3 Select expression
        :param expression_type: S3 Select expression type
        :param input_serialization: S3 Select input data serialization format
        :param output_serialization: S3 Select output data serialization format
        :return: retrieved subset of original data by S3 Select
        """
        expression = expression or "SELECT * FROM S3Object"
        expression_type = expression_type or "SQL"
        extra_args = {}

        if input_serialization is None:
            input_serialization = {"CSV": {}}
        if output_serialization is None:
            output_serialization = {"CSV": {}}
        if self._requester_pays:
            extra_args["RequestPayer"] = "requester"

        response = self.get_conn().select_object_content(
            Bucket=bucket_name,
            Key=key,
            Expression=expression,
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
            ExtraArgs=extra_args,
        )

        return b"".join(
            event["Records"]["Payload"] for event in response["Payload"] if "Records" in event
        ).decode("utf-8")

    @unify_bucket_name_and_key
    @provide_bucket_name
    def check_for_wildcard_key(
        self, wildcard_key: str, bucket_name: str | None = None, delimiter: str = ""
    ) -> bool:
        """
        Check that a key matching a wildcard expression exists in a bucket.

        :param wildcard_key: the path to the key
        :param bucket_name: the name of the bucket
        :param delimiter: the delimiter marks key hierarchy
        :return: True if a key exists and False if not.
        """
        return (
            self.get_wildcard_key(wildcard_key=wildcard_key, bucket_name=bucket_name, delimiter=delimiter)
            is not None
        )

    @unify_bucket_name_and_key
    @provide_bucket_name
    def get_wildcard_key(
        self, wildcard_key: str, bucket_name: str | None = None, delimiter: str = ""
    ) -> S3ResourceObject | None:
        """
        Return a boto3.s3.Object object matching the wildcard expression.

        :param wildcard_key: the path to the key
        :param bucket_name: the name of the bucket
        :param delimiter: the delimiter marks key hierarchy
        :return: the key object from the bucket or None if none has been found.
        """
        prefix = re.split(r"[\[*?]", wildcard_key, 1)[0]
        key_list = self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        key_matches = [k for k in key_list if fnmatch.fnmatch(k, wildcard_key)]
        if key_matches:
            return self.get_key(key_matches[0], bucket_name)
        return None

    @unify_bucket_name_and_key
    @provide_bucket_name
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
        Load a local file to S3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.upload_file`

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
            extra_args["ServerSideEncryption"] = "AES256"
        if gzip:
            with open(filename, "rb") as f_in:
                filename_gz = f"{f_in.name}.gz"
                with gz.open(filename_gz, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz
        if acl_policy:
            extra_args["ACL"] = acl_policy
        if self._requester_pays:
            extra_args["RequestPayer"] = "requester"

        client = self.get_conn()
        client.upload_file(
            filename,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )
        get_hook_lineage_collector().add_input_asset(
            context=self, scheme="file", asset_kwargs={"path": filename}
        )
        get_hook_lineage_collector().add_output_asset(
            context=self, scheme="s3", asset_kwargs={"bucket": bucket_name, "key": key}
        )

    @unify_bucket_name_and_key
    @provide_bucket_name
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
        Load a string to S3.

        This is provided as a convenience to drop a string in S3. It uses the
        boto infrastructure to ship a file to s3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.upload_fileobj`

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
        encoding = encoding or "utf-8"

        bytes_data = string_data.encode(encoding)

        # Compress string
        available_compressions = ["gzip"]
        if compression is not None and compression not in available_compressions:
            raise NotImplementedError(
                f"Received {compression} compression type. "
                f"String can currently be compressed in {available_compressions} only."
            )
        if compression == "gzip":
            bytes_data = gz.compress(bytes_data)

        with BytesIO(bytes_data) as f:
            self._upload_file_obj(f, key, bucket_name, replace, encrypt, acl_policy)

    @unify_bucket_name_and_key
    @provide_bucket_name
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
        Load bytes to S3.

        This is provided as a convenience to drop bytes data into S3. It uses the
        boto infrastructure to ship a file to s3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.upload_fileobj`

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
        with BytesIO(bytes_data) as f:
            self._upload_file_obj(f, key, bucket_name, replace, encrypt, acl_policy)

    @unify_bucket_name_and_key
    @provide_bucket_name
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
        Load a file object to S3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.upload_fileobj`

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
            extra_args["ServerSideEncryption"] = "AES256"
        if acl_policy:
            extra_args["ACL"] = acl_policy
        if self._requester_pays:
            extra_args["RequestPayer"] = "requester"

        client = self.get_conn()
        client.upload_fileobj(
            file_obj,
            bucket_name,
            key,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )
        # No input because file_obj can be anything - handle in calling function if possible
        get_hook_lineage_collector().add_output_asset(
            context=self, scheme="s3", asset_kwargs={"bucket": bucket_name, "key": key}
        )

    def copy_object(
        self,
        source_bucket_key: str,
        dest_bucket_key: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        source_version_id: str | None = None,
        acl_policy: str | None = None,
        meta_data_directive: str | None = None,
        **kwargs,
    ) -> None:
        """
        Create a copy of an object that is already stored in S3.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.copy_object`

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
        :param meta_data_directive: Whether to `COPY` the metadata from the source object or `REPLACE` it
            with metadata that's provided in the request.
        """
        acl_policy = acl_policy or "private"
        if acl_policy != NO_ACL:
            kwargs["ACL"] = acl_policy
        if meta_data_directive:
            kwargs["MetadataDirective"] = meta_data_directive
        if self._requester_pays:
            kwargs["RequestPayer"] = "requester"

        dest_bucket_name, dest_bucket_key = self.get_s3_bucket_key(
            dest_bucket_name, dest_bucket_key, "dest_bucket_name", "dest_bucket_key"
        )

        source_bucket_name, source_bucket_key = self.get_s3_bucket_key(
            source_bucket_name,
            source_bucket_key,
            "source_bucket_name",
            "source_bucket_key",
        )

        copy_source = {
            "Bucket": source_bucket_name,
            "Key": source_bucket_key,
            "VersionId": source_version_id,
        }
        response = self.get_conn().copy_object(
            Bucket=dest_bucket_name,
            Key=dest_bucket_key,
            CopySource=copy_source,
            **kwargs,
        )
        get_hook_lineage_collector().add_input_asset(
            context=self,
            scheme="s3",
            asset_kwargs={"bucket": source_bucket_name, "key": source_bucket_key},
        )
        get_hook_lineage_collector().add_output_asset(
            context=self,
            scheme="s3",
            asset_kwargs={"bucket": dest_bucket_name, "key": dest_bucket_key},
        )
        return response

    @provide_bucket_name
    def delete_bucket(self, bucket_name: str, force_delete: bool = False, max_retries: int = 5) -> None:
        """
        To delete s3 bucket, delete all s3 bucket objects and then delete the bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.delete_bucket`

        :param bucket_name: Bucket name
        :param force_delete: Enable this to delete bucket even if not empty
        :param max_retries: A bucket must be empty to be deleted.  If force_delete is true,
            then retries may help prevent a race condition between deleting objects in the
            bucket and trying to delete the bucket.
        :return: None
        """
        if force_delete:
            for retry in range(max_retries):
                bucket_keys = self.list_keys(bucket_name=bucket_name)
                if not bucket_keys:
                    break
                if retry:  # Avoid first loop
                    time.sleep(500)

                self.delete_objects(bucket=bucket_name, keys=bucket_keys)

        self.conn.delete_bucket(Bucket=bucket_name)

    def delete_objects(self, bucket: str, keys: str | list) -> None:
        """
        Delete keys from the bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.delete_objects`

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
        extra_kwargs = {}
        if self._requester_pays:
            extra_kwargs["RequestPayer"] = "requester"

        # We can only send a maximum of 1000 keys per request.
        # For details see:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        for chunk in chunks(keys, chunk_size=1000):
            response = s3.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]}, **extra_kwargs
            )
            deleted_keys = [x["Key"] for x in response.get("Deleted", [])]
            self.log.info("Deleted: %s", deleted_keys)
            if "Errors" in response:
                errors_keys = [x["Key"] for x in response.get("Errors", [])]
                raise AirflowException(f"Errors when deleting: {errors_keys}")

    @unify_bucket_name_and_key
    @provide_bucket_name
    def download_file(
        self,
        key: str,
        bucket_name: str | None = None,
        local_path: str | None = None,
        preserve_file_name: bool = False,
        use_autogenerated_subdir: bool = True,
    ) -> str:
        """
        Download a file from the S3 location to the local file system.

        Note:
            This function shadows the 'download_file' method of S3 API, but it is not the same.
            If you want to use the original method from S3 API, please use 'S3Hook.get_conn().download_file()'

        .. seealso::
            - :external+boto3:py:meth:`S3.Object.download_fileobj`

        :param key: The key path in S3.
        :param bucket_name: The specific bucket to use.
        :param local_path: The local path to the downloaded file. If no path is provided it will use the
            system's temporary directory.
        :param preserve_file_name: If you want the downloaded file name to be the same name as it is in S3,
            set this parameter to True. When set to False, a random filename will be generated.
            Default: False.
        :param use_autogenerated_subdir: Pairs with 'preserve_file_name = True' to download the file into a
            random generated folder inside the 'local_path', useful to avoid collisions between various tasks
            that might download the same file name. Set it to 'False' if you don't want it, and you want a
            predictable path.
            Default: True.
        :return: the file name.
        """
        self.log.info("Downloading source S3 file from Bucket %s with path %s", bucket_name, key)

        try:
            s3_obj = self.get_key(key, bucket_name)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == 404:
                raise AirflowNotFoundException(
                    f"The source file in Bucket {bucket_name} with path {key} does not exist"
                )
            raise e

        if preserve_file_name:
            local_dir = local_path or gettempdir()
            subdir = f"airflow_tmp_dir_{uuid4().hex[0:8]}" if use_autogenerated_subdir else ""
            filename_in_s3 = s3_obj.key.rsplit("/", 1)[-1]
            file_path = Path(local_dir, subdir, filename_in_s3)

            if file_path.is_file():
                self.log.error(
                    "file '%s' already exists. Failing the task and not overwriting it",
                    file_path,
                )
                raise FileExistsError

            file_path.parent.mkdir(exist_ok=True, parents=True)

            get_hook_lineage_collector().add_output_asset(
                context=self,
                scheme="file",
                asset_kwargs={
                    "path": str(file_path) if file_path.is_absolute() else str(file_path.absolute())
                },
            )
            file = open(file_path, "wb")
        else:
            file = NamedTemporaryFile(dir=local_path, prefix="airflow_tmp_", delete=False)  # type: ignore

        extra_args = {**self.extra_args}
        if self._requester_pays:
            extra_args["RequestPayer"] = "requester"
        s3_obj.download_fileobj(
            file,
            ExtraArgs=extra_args,
            Config=self.transfer_config,
        )
        get_hook_lineage_collector().add_input_asset(
            context=self, scheme="s3", asset_kwargs={"bucket": bucket_name, "key": key}
        )
        return file.name

    def generate_presigned_url(
        self,
        client_method: str,
        params: dict | None = None,
        expires_in: int = 3600,
        http_method: str | None = None,
    ) -> str | None:
        """
        Generate a presigned url given a client, its method, and arguments.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.generate_presigned_url`

        :param client_method: The client method to presign for.
        :param params: The parameters normally passed to ClientMethod.
        :param expires_in: The number of seconds the presigned url is valid for.
            By default it expires in an hour (3600 seconds).
        :param http_method: The http method to use on the generated url.
            By default, the http method is whatever is used in the method's model.
        :return: The presigned url.
        """
        s3_client = self.get_conn()
        try:
            return s3_client.generate_presigned_url(
                ClientMethod=client_method,
                Params=params,
                ExpiresIn=expires_in,
                HttpMethod=http_method,
            )

        except ClientError as e:
            self.log.error(e.response["Error"]["Message"])
            return None

    @provide_bucket_name
    def get_bucket_tagging(self, bucket_name: str | None = None) -> list[dict[str, str]] | None:
        """
        Get a List of tags from a bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.get_bucket_tagging`

        :param bucket_name: The name of the bucket.
        :return: A List containing the key/value pairs for the tags
        """
        try:
            s3_client = self.get_conn()
            result = s3_client.get_bucket_tagging(Bucket=bucket_name)["TagSet"]
            self.log.info("S3 Bucket Tag Info: %s", result)
            return result
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def put_bucket_tagging(
        self,
        tag_set: dict[str, str] | list[dict[str, str]] | None = None,
        key: str | None = None,
        value: str | None = None,
        bucket_name: str | None = None,
    ) -> None:
        """
        Overwrite the existing TagSet with provided tags; must provide a TagSet, a key/value pair, or both.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.put_bucket_tagging`

        :param tag_set: A dictionary containing the key/value pairs for the tags,
            or a list already formatted for the API
        :param key: The Key for the new TagSet entry.
        :param value: The Value for the new TagSet entry.
        :param bucket_name: The name of the bucket.

        :return: None
        """
        formatted_tags = format_tags(tag_set)

        if key and value:
            formatted_tags.append({"Key": key, "Value": value})
        elif key or value:
            message = (
                "Key and Value must be specified as a pair. "
                f"Only one of the two had a value (key: '{key}', value: '{value}')"
            )
            self.log.error(message)
            raise ValueError(message)

        self.log.info("Tagging S3 Bucket %s with %s", bucket_name, formatted_tags)

        try:
            s3_client = self.get_conn()
            s3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": formatted_tags})
        except ClientError as e:
            self.log.error(e)
            raise e

    @provide_bucket_name
    def delete_bucket_tagging(self, bucket_name: str | None = None) -> None:
        """
        Delete all tags from a bucket.

        .. seealso::
            - :external+boto3:py:meth:`S3.Client.delete_bucket_tagging`

        :param bucket_name: The name of the bucket.
        :return: None
        """
        s3_client = self.get_conn()
        s3_client.delete_bucket_tagging(Bucket=bucket_name)

    def _sync_to_local_dir_delete_stale_local_files(self, current_s3_objects: list[Path], local_dir: Path):
        current_s3_keys = {key for key in current_s3_objects}

        for item in local_dir.iterdir():
            item: Path  # type: ignore[no-redef]
            absolute_item_path = item.resolve()

            if absolute_item_path not in current_s3_keys:
                try:
                    if item.is_file():
                        item.unlink(missing_ok=True)
                        self.log.debug("Deleted stale local file: %s", item)
                    elif item.is_dir():
                        # delete only when the folder is empty
                        if not os.listdir(item):
                            item.rmdir()
                            self.log.debug("Deleted stale empty directory: %s", item)
                    else:
                        self.log.debug("Skipping stale item of unknown type: %s", item)
                except OSError as e:
                    self.log.error("Error deleting stale item %s: %s", item, e)
                    raise e

    def _compute_local_file_md5(self, file_path: Path) -> str:
        hash_md5 = hashlib.md5(usedforsecurity=False)
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
        except (FileNotFoundError, PermissionError, OSError) as e:
            self.log.error("Failed to compute MD5 for local file %s: %s", file_path, e)
            raise AirflowException(f"Failed to compute MD5 for local file {file_path}: {e}") from e
        return f'"{hash_md5.hexdigest()}"'

    def _sync_to_local_dir_if_changed(self, s3_bucket, s3_object, local_target_path: Path):
        should_download = False
        download_msg = ""
        if not local_target_path.exists():
            should_download = True
            download_msg = f"Local file {local_target_path} does not exist."
        else:
            local_stats = local_target_path.stat()

            if s3_object.size != local_stats.st_size:
                should_download = True
                download_msg = (
                    f"S3 object size ({s3_object.size}) and local file size ({local_stats.st_size}) differ."
                )
            else:
                s3_etag = s3_object.e_tag
                if s3_etag:
                    if "-" not in s3_etag:
                        local_md5 = self._compute_local_file_md5(local_target_path)
                        if local_md5 != s3_etag:
                            should_download = True
                            download_msg = (
                                f"S3 object ETag ({s3_etag}) and local file MD5 ({local_md5}) differ "
                                f"(content changed while size remained the same)."
                            )
                    else:
                        s3_last_modified = s3_object.last_modified
                        if s3_last_modified and local_stats.st_mtime < s3_last_modified.timestamp():
                            should_download = True
                            download_msg = (
                                f"S3 object last modified ({s3_last_modified}) is newer than "
                                f"local file last modified ({datetime.fromtimestamp(local_stats.st_mtime)})."
                            )
                else:
                    s3_last_modified = s3_object.last_modified
                    if s3_last_modified and local_stats.st_mtime < s3_last_modified.timestamp():
                        should_download = True
                        download_msg = (
                            f"S3 object last modified ({s3_last_modified}) is newer than "
                            f"local file last modified ({datetime.fromtimestamp(local_stats.st_mtime)})."
                        )

        if should_download:
            s3_bucket.download_file(s3_object.key, local_target_path)
            self.log.debug(
                "%s Downloaded %s to %s", download_msg, s3_object.key, local_target_path.as_posix()
            )
        else:
            self.log.debug(
                "Local file %s is up-to-date with S3 object %s. Skipping download.",
                local_target_path.as_posix(),
                s3_object.key,
            )

    def sync_to_local_dir(self, bucket_name: str, local_dir: Path, s3_prefix="", delete_stale: bool = True):
        """Download S3 files from the S3 bucket to the local directory."""
        self.log.debug("Downloading data from s3://%s/%s to %s", bucket_name, s3_prefix, local_dir)

        local_s3_objects = []
        s3_bucket = self.get_bucket(bucket_name)
        for obj in s3_bucket.objects.filter(Prefix=s3_prefix):
            if obj.key.endswith("/"):
                continue
            obj_path = Path(obj.key)
            local_target_path = local_dir.joinpath(obj_path.relative_to(s3_prefix))
            if not local_target_path.parent.exists():
                local_target_path.parent.mkdir(parents=True, exist_ok=True)
                self.log.debug("Created local directory: %s", local_target_path.parent)
            self._sync_to_local_dir_if_changed(
                s3_bucket=s3_bucket, s3_object=obj, local_target_path=local_target_path
            )
            local_s3_objects.append(local_target_path)

        if delete_stale:
            self._sync_to_local_dir_delete_stale_local_files(
                current_s3_objects=local_s3_objects, local_dir=local_dir
            )
