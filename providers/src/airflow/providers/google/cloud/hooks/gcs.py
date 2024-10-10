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
"""This module contains a Google Cloud Storage hook."""

from __future__ import annotations

import functools
import gzip as gz
import json
import os
import shutil
import time
import warnings
from contextlib import contextmanager
from functools import partial
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Callable, Generator, Sequence, TypeVar, cast, overload
from urllib.parse import urlsplit

from gcloud.aio.storage import Storage
from google.api_core.exceptions import GoogleAPICallError, NotFound

# not sure why but mypy complains on missing `storage` but it is clearly there and is importable
from google.cloud import storage  # type: ignore[attr-defined]
from google.cloud.exceptions import GoogleCloudError
from google.cloud.storage.retry import DEFAULT_RETRY
from requests import Session

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.utils.helpers import normalize_directory_path
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)
from airflow.typing_compat import ParamSpec
from airflow.utils import timezone
from airflow.version import version

if TYPE_CHECKING:
    from datetime import datetime

    from aiohttp import ClientSession
    from google.api_core.retry import Retry
    from google.cloud.storage.blob import Blob


RT = TypeVar("RT")
T = TypeVar("T", bound=Callable)
FParams = ParamSpec("FParams")

# GCSHook has a method named 'list' (to junior devs: please don't do this), so
# we need to create an alias to prevent Mypy being confused.
List = list

# Use default timeout from google-cloud-storage
DEFAULT_TIMEOUT = 60


def _fallback_object_url_to_object_name_and_bucket_name(
    object_url_keyword_arg_name="object_url",
    bucket_name_keyword_arg_name="bucket_name",
    object_name_keyword_arg_name="object_name",
) -> Callable[[T], T]:
    """
    Convert object URL parameter to object name and bucket name parameter.

    This method is a Decorator factory.

    :param object_url_keyword_arg_name: Name of the object URL parameter
    :param bucket_name_keyword_arg_name: Name of the bucket name parameter
    :param object_name_keyword_arg_name: Name of the object name parameter
    :return: Decorator
    """

    def _wrapper(func: Callable[FParams, RT]) -> Callable[FParams, RT]:
        @functools.wraps(func)
        def _inner_wrapper(self, *args, **kwargs) -> RT:
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            object_url = kwargs.get(object_url_keyword_arg_name)
            bucket_name = kwargs.get(bucket_name_keyword_arg_name)
            object_name = kwargs.get(object_name_keyword_arg_name)

            if object_url and bucket_name and object_name:
                raise AirflowException(
                    "The mutually exclusive parameters. `object_url`, `bucket_name` together "
                    "with `object_name` parameters are present. "
                    "Please provide `object_url` or `bucket_name` and `object_name`."
                )
            if object_url:
                bucket_name, object_name = _parse_gcs_url(object_url)
                kwargs[bucket_name_keyword_arg_name] = bucket_name
                kwargs[object_name_keyword_arg_name] = object_name
                del kwargs[object_url_keyword_arg_name]

            if not object_name or not bucket_name:
                raise TypeError(
                    f"{func.__name__}() missing 2 required positional arguments: "
                    f"'{bucket_name_keyword_arg_name}' and '{object_name_keyword_arg_name}' "
                    f"or {object_url_keyword_arg_name}"
                )
            if not object_name:
                raise TypeError(
                    f"{func.__name__}() missing 1 required positional argument: "
                    f"'{object_name_keyword_arg_name}'"
                )
            if not bucket_name:
                raise TypeError(
                    f"{func.__name__}() missing 1 required positional argument: "
                    f"'{bucket_name_keyword_arg_name}'"
                )

            return func(self, *args, **kwargs)

        return cast(Callable[FParams, RT], _inner_wrapper)

    return cast(Callable[[T], T], _wrapper)


# A fake bucket to use in functions decorated by _fallback_object_url_to_object_name_and_bucket_name.
# This allows the 'bucket' argument to be of type str instead of str | None,
# making it easier to type hint the function body without dealing with the None
# case that can never happen at runtime.
PROVIDE_BUCKET: str = cast(str, None)


class GCSHook(GoogleBaseHook):
    """Use the Google Cloud connection to interact with Google Cloud Storage."""

    _conn: storage.Client | None = None

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )

        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> storage.Client:
        """Return a Google Cloud Storage service object."""
        if not self._conn:
            self._conn = storage.Client(
                credentials=self.get_credentials(), client_info=CLIENT_INFO, project=self.project_id
            )

        return self._conn

    def copy(
        self,
        source_bucket: str,
        source_object: str,
        destination_bucket: str | None = None,
        destination_object: str | None = None,
    ) -> None:
        """
        Copy an object from a bucket to another, with renaming if requested.

        destination_bucket or destination_object can be omitted, in which case
        source bucket/object is used, but not both.

        :param source_bucket: The bucket of the object to copy from.
        :param source_object: The object to copy.
        :param destination_bucket: The destination of the object to copied to.
            Can be omitted; then the same bucket is used.
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        """
        destination_bucket = destination_bucket or source_bucket
        destination_object = destination_object or source_object

        if source_bucket == destination_bucket and source_object == destination_object:
            raise ValueError(
                f"Either source/destination bucket or source/destination object must be different, "
                f"not both the same: bucket={source_bucket}, object={source_object}"
            )
        if not source_bucket or not source_object:
            raise ValueError("source_bucket and source_object cannot be empty.")

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(source_object)  # type: ignore[attr-defined]
        destination_bucket = client.bucket(destination_bucket)
        destination_object = source_bucket.copy_blob(  # type: ignore[attr-defined]
            blob=source_object, destination_bucket=destination_bucket, new_name=destination_object
        )

        self.log.info(
            "Object %s in bucket %s copied to object %s in bucket %s",
            source_object.name,  # type: ignore[attr-defined]
            source_bucket.name,  # type: ignore[attr-defined]
            destination_object.name,  # type: ignore[union-attr]
            destination_bucket.name,  # type: ignore[union-attr]
        )

    def rewrite(
        self,
        source_bucket: str,
        source_object: str,
        destination_bucket: str,
        destination_object: str | None = None,
    ) -> None:
        """
        Similar to copy; supports files over 5 TB, and copying between locations and/or storage classes.

        destination_object can be omitted, in which case source_object is used.

        :param source_bucket: The bucket of the object to copy from.
        :param source_object: The object to copy.
        :param destination_bucket: The destination of the object to copied to.
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        """
        destination_object = destination_object or source_object
        if source_bucket == destination_bucket and source_object == destination_object:
            raise ValueError(
                f"Either source/destination bucket or source/destination object must be different, "
                f"not both the same: bucket={source_bucket}, object={source_object}"
            )
        if not source_bucket or not source_object:
            raise ValueError("source_bucket and source_object cannot be empty.")

        client = self.get_conn()
        source_bucket = client.bucket(source_bucket)
        source_object = source_bucket.blob(blob_name=source_object)  # type: ignore[attr-defined]
        destination_bucket = client.bucket(destination_bucket)

        token, bytes_rewritten, total_bytes = destination_bucket.blob(  # type: ignore[attr-defined]
            blob_name=destination_object
        ).rewrite(source=source_object)

        self.log.info("Total Bytes: %s | Bytes Written: %s", total_bytes, bytes_rewritten)

        while token is not None:
            token, bytes_rewritten, total_bytes = destination_bucket.blob(  # type: ignore[attr-defined]
                blob_name=destination_object
            ).rewrite(source=source_object, token=token)

            self.log.info("Total Bytes: %s | Bytes Written: %s", total_bytes, bytes_rewritten)
        self.log.info(
            "Object %s in bucket %s rewritten to object %s in bucket %s",
            source_object.name,  # type: ignore[attr-defined]
            source_bucket.name,  # type: ignore[attr-defined]
            destination_object,
            destination_bucket.name,  # type: ignore[attr-defined]
        )

    @overload
    def download(
        self,
        bucket_name: str,
        object_name: str,
        filename: None = None,
        chunk_size: int | None = None,
        timeout: int | None = DEFAULT_TIMEOUT,
        num_max_attempts: int | None = 1,
        user_project: str | None = None,
    ) -> bytes: ...

    @overload
    def download(
        self,
        bucket_name: str,
        object_name: str,
        filename: str,
        chunk_size: int | None = None,
        timeout: int | None = DEFAULT_TIMEOUT,
        num_max_attempts: int | None = 1,
        user_project: str | None = None,
    ) -> str: ...

    def download(
        self,
        bucket_name: str,
        object_name: str,
        filename: str | None = None,
        chunk_size: int | None = None,
        timeout: int | None = DEFAULT_TIMEOUT,
        num_max_attempts: int | None = 1,
        user_project: str | None = None,
    ) -> str | bytes:
        """
        Download a file from Google Cloud Storage.

        When no filename is supplied, the operator loads the file into memory and returns its
        content. When a filename is supplied, it writes the file to the specified location and
        returns the location. For file sizes that exceed the available memory it is recommended
        to write to a file.

        :param bucket_name: The bucket to fetch from.
        :param object_name: The object to fetch.
        :param filename: If set, a local file path where the file should be written to.
        :param chunk_size: Blob chunk size.
        :param timeout: Request timeout in seconds.
        :param num_max_attempts: Number of attempts to download the file.
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        """
        # TODO: future improvement check file size before downloading,
        #  to check for local space availability

        if num_max_attempts is None:
            num_max_attempts = 3

        for attempt in range(num_max_attempts):
            if attempt:
                # Wait with exponential backoff scheme before retrying.
                timeout_seconds = 2**attempt
                time.sleep(timeout_seconds)

            try:
                client = self.get_conn()
                bucket = client.bucket(bucket_name, user_project=user_project)
                blob = bucket.blob(blob_name=object_name, chunk_size=chunk_size)

                if filename:
                    blob.download_to_filename(filename, timeout=timeout)
                    self.log.info("File downloaded to %s", filename)
                    return filename
                else:
                    return blob.download_as_bytes()

            except GoogleCloudError:
                if attempt == num_max_attempts - 1:
                    self.log.error(
                        "Download attempt of object: %s from %s has failed. Attempt: %s, max %s.",
                        object_name,
                        bucket_name,
                        attempt,
                        num_max_attempts,
                    )
                    raise
        else:
            raise NotImplementedError  # should not reach this, but makes mypy happy

    def download_as_byte_array(
        self,
        bucket_name: str,
        object_name: str,
        chunk_size: int | None = None,
        timeout: int | None = DEFAULT_TIMEOUT,
        num_max_attempts: int | None = 1,
    ) -> bytes:
        """
        Download a file from Google Cloud Storage.

        When no filename is supplied, the operator loads the file into memory and returns its
        content. When a filename is supplied, it writes the file to the specified location and
        returns the location. For file sizes that exceed the available memory it is recommended
        to write to a file.

        :param bucket_name: The bucket to fetch from.
        :param object_name: The object to fetch.
        :param chunk_size: Blob chunk size.
        :param timeout: Request timeout in seconds.
        :param num_max_attempts: Number of attempts to download the file.
        """
        # We do not pass filename, so will never receive string as response
        return self.download(
            bucket_name=bucket_name,
            object_name=object_name,
            chunk_size=chunk_size,
            timeout=timeout,
            num_max_attempts=num_max_attempts,
        )

    @_fallback_object_url_to_object_name_and_bucket_name()
    @contextmanager
    def provide_file(
        self,
        bucket_name: str = PROVIDE_BUCKET,
        object_name: str | None = None,
        object_url: str | None = None,
        dir: str | None = None,
        user_project: str | None = None,
    ) -> Generator[IO[bytes], None, None]:
        """
        Download the file to a temporary directory and returns a file handle.

        You can use this method by passing the bucket_name and object_name parameters
        or just object_url parameter.

        :param bucket_name: The bucket to fetch from.
        :param object_name: The object to fetch.
        :param object_url: File reference url. Must start with "gs: //"
        :param dir: The tmp sub directory to download the file to. (passed to NamedTemporaryFile)
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        :return: File handler
        """
        if object_name is None:
            raise ValueError("Object name can not be empty")
        _, _, file_name = object_name.rpartition("/")
        with NamedTemporaryFile(suffix=file_name, dir=dir) as tmp_file:
            self.download(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=tmp_file.name,
                user_project=user_project,
            )
            tmp_file.flush()
            yield tmp_file

    @_fallback_object_url_to_object_name_and_bucket_name()
    @contextmanager
    def provide_file_and_upload(
        self,
        bucket_name: str = PROVIDE_BUCKET,
        object_name: str | None = None,
        object_url: str | None = None,
        user_project: str | None = None,
    ) -> Generator[IO[bytes], None, None]:
        """
        Create temporary file, returns a file handle and uploads the files content on close.

        You can use this method by passing the bucket_name and object_name parameters
        or just object_url parameter.

        :param bucket_name: The bucket to fetch from.
        :param object_name: The object to fetch.
        :param object_url: File reference url. Must start with "gs: //"
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        :return: File handler
        """
        if object_name is None:
            raise ValueError("Object name can not be empty")

        _, _, file_name = object_name.rpartition("/")
        with NamedTemporaryFile(suffix=file_name) as tmp_file:
            yield tmp_file
            tmp_file.flush()
            self.upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=tmp_file.name,
                user_project=user_project,
            )

    def upload(
        self,
        bucket_name: str,
        object_name: str,
        filename: str | None = None,
        data: str | bytes | None = None,
        mime_type: str | None = None,
        gzip: bool = False,
        encoding: str = "utf-8",
        chunk_size: int | None = None,
        timeout: int | None = DEFAULT_TIMEOUT,
        num_max_attempts: int = 1,
        metadata: dict | None = None,
        cache_control: str | None = None,
        user_project: str | None = None,
    ) -> None:
        """
        Upload a local file or file data as string or bytes to Google Cloud Storage.

        :param bucket_name: The bucket to upload to.
        :param object_name: The object name to set when uploading the file.
        :param filename: The local file path to the file to be uploaded.
        :param data: The file's data as a string or bytes to be uploaded.
        :param mime_type: The file's mime type set when uploading the file.
        :param gzip: Option to compress local file or file data for upload
        :param encoding: bytes encoding for file data if provided as string
        :param chunk_size: Blob chunk size.
        :param timeout: Request timeout in seconds.
        :param num_max_attempts: Number of attempts to try to upload the file.
        :param metadata: The metadata to be uploaded with the file.
        :param cache_control: Cache-Control metadata field.
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        """

        def _call_with_retry(f: Callable[[], None]) -> None:
            """
            Upload a file or a string with a retry mechanism and exponential back-off.

            :param f: Callable that should be retried.
            """
            for attempt in range(1, 1 + num_max_attempts):
                try:
                    f()
                except GoogleCloudError as e:
                    if attempt == num_max_attempts:
                        self.log.error(
                            "Upload attempt of object: %s from %s has failed. Attempt: %s, max %s.",
                            object_name,
                            object_name,
                            attempt,
                            num_max_attempts,
                        )
                        raise e

                    # Wait with exponential backoff scheme before retrying.
                    timeout_seconds = 2 ** (attempt - 1)
                    time.sleep(timeout_seconds)

        client = self.get_conn()
        bucket = client.bucket(bucket_name, user_project=user_project)
        blob = bucket.blob(blob_name=object_name, chunk_size=chunk_size)

        if metadata:
            blob.metadata = metadata

        if cache_control:
            blob.cache_control = cache_control

        if filename and data:
            raise ValueError(
                "'filename' and 'data' parameter provided. Please "
                "specify a single parameter, either 'filename' for "
                "local file uploads or 'data' for file content uploads."
            )
        elif filename:
            if not mime_type:
                mime_type = "application/octet-stream"
            if gzip:
                filename_gz = filename + ".gz"

                with open(filename, "rb") as f_in, gz.open(filename_gz, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz

            _call_with_retry(
                partial(blob.upload_from_filename, filename=filename, content_type=mime_type, timeout=timeout)
            )

            if gzip:
                os.remove(filename)
            self.log.info("File %s uploaded to %s in %s bucket", filename, object_name, bucket_name)
        elif data:
            if not mime_type:
                mime_type = "text/plain"
            if gzip:
                if isinstance(data, str):
                    data = bytes(data, encoding)
                out = BytesIO()
                with gz.GzipFile(fileobj=out, mode="w") as f:
                    f.write(data)
                data = out.getvalue()

            _call_with_retry(partial(blob.upload_from_string, data, content_type=mime_type, timeout=timeout))

            self.log.info("Data stream uploaded to %s in %s bucket", object_name, bucket_name)
        else:
            raise ValueError("'filename' and 'data' parameter missing. One is required to upload to gcs.")

    def exists(self, bucket_name: str, object_name: str, retry: Retry = DEFAULT_RETRY) -> bool:
        """
        Check for the existence of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        :param retry: (Optional) How to retry the RPC
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name)
        return blob.exists(retry=retry)

    def get_blob_update_time(self, bucket_name: str, object_name: str):
        """
        Get the update time of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob to get updated time from the Google cloud
            storage bucket.
        """
        blob = self._get_blob(bucket_name, object_name)
        return blob.updated

    def is_updated_after(self, bucket_name: str, object_name: str, ts: datetime) -> bool:
        """
        Check if an blob_name is updated in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :param ts: The timestamp to check against.
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s > %s", blob_update_time, ts)
            if blob_update_time > ts:
                return True
        return False

    def is_updated_between(
        self, bucket_name: str, object_name: str, min_ts: datetime, max_ts: datetime
    ) -> bool:
        """
        Check if an blob_name is updated in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the object to check in the Google cloud
                storage bucket.
        :param min_ts: The minimum timestamp to check against.
        :param max_ts: The maximum timestamp to check against.
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:
            if not min_ts.tzinfo:
                min_ts = min_ts.replace(tzinfo=timezone.utc)
            if not max_ts.tzinfo:
                max_ts = max_ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s is between %s and %s", blob_update_time, min_ts, max_ts)
            if min_ts <= blob_update_time < max_ts:
                return True
        return False

    def is_updated_before(self, bucket_name: str, object_name: str, ts: datetime) -> bool:
        """
        Check if an blob_name is updated before given time in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :param ts: The timestamp to check against.
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=timezone.utc)
            self.log.info("Verify object date: %s < %s", blob_update_time, ts)
            if blob_update_time < ts:
                return True
        return False

    def is_older_than(self, bucket_name: str, object_name: str, seconds: int) -> bool:
        """
        Check if object is older than given time.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the object to check in the Google cloud
            storage bucket.
        :param seconds: The time in seconds to check against
        """
        blob_update_time = self.get_blob_update_time(bucket_name, object_name)
        if blob_update_time is not None:
            from datetime import timedelta

            current_time = timezone.utcnow()
            given_time = current_time - timedelta(seconds=seconds)
            self.log.info("Verify object date: %s is older than %s", blob_update_time, given_time)
            if blob_update_time < given_time:
                return True
        return False

    def delete(self, bucket_name: str, object_name: str) -> None:
        """
        Delete an object from the bucket.

        :param bucket_name: name of the bucket, where the object resides
        :param object_name: name of the object to delete
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name=object_name)
        blob.delete()

        self.log.info("Blob %s deleted.", object_name)

    def delete_bucket(self, bucket_name: str, force: bool = False, user_project: str | None = None) -> None:
        """
        Delete a bucket object from the Google Cloud Storage.

        :param bucket_name: name of the bucket which will be deleted
        :param force: false not allow to delete non empty bucket, set force=True
            allows to delete non empty bucket
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name, user_project=user_project)

        self.log.info("Deleting %s bucket", bucket_name)
        try:
            bucket.delete(force=force)
            self.log.info("Bucket %s has been deleted", bucket_name)
        except NotFound:
            self.log.info("Bucket %s not exists", bucket_name)

    def list(
        self,
        bucket_name: str,
        versions: bool | None = None,
        max_results: int | None = None,
        prefix: str | List[str] | None = None,
        delimiter: str | None = None,
        match_glob: str | None = None,
        user_project: str | None = None,
    ):
        """
        List all objects from the bucket with the given a single prefix or multiple prefixes.

        :param bucket_name: bucket name
        :param versions: if true, list all versions of the objects
        :param max_results: max count of items to return in a single page of responses
        :param prefix: string or list of strings which filter objects whose name begin with it/them
        :param delimiter: (Deprecated) filters objects based on the delimiter (for e.g '.csv')
        :param match_glob: (Optional) filters objects based on the glob pattern given by the string
            (e.g, ``'**/*/.json'``).
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        :return: a stream of object names matching the filtering criteria
        """
        if delimiter and delimiter != "/":
            warnings.warn(
                "Usage of 'delimiter' param is deprecated, please use 'match_glob' instead",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        if match_glob and delimiter and delimiter != "/":
            raise AirflowException("'match_glob' param cannot be used with 'delimiter' that differs than '/'")
        objects = []
        if isinstance(prefix, list):
            for prefix_item in prefix:
                objects.extend(
                    self._list(
                        bucket_name=bucket_name,
                        versions=versions,
                        max_results=max_results,
                        prefix=prefix_item,
                        delimiter=delimiter,
                        match_glob=match_glob,
                        user_project=user_project,
                    )
                )
        else:
            objects.extend(
                self._list(
                    bucket_name=bucket_name,
                    versions=versions,
                    max_results=max_results,
                    prefix=prefix,
                    delimiter=delimiter,
                    match_glob=match_glob,
                    user_project=user_project,
                )
            )
        return objects

    def _list(
        self,
        bucket_name: str,
        versions: bool | None = None,
        max_results: int | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        match_glob: str | None = None,
        user_project: str | None = None,
    ) -> List:
        """
        List all objects from the bucket with the give string prefix in name.

        :param bucket_name: bucket name
        :param versions: if true, list all versions of the objects
        :param max_results: max count of items to return in a single page of responses
        :param prefix: string which filters objects whose name begin with it
        :param delimiter: (Deprecated) filters objects based on the delimiter (for e.g '.csv')
        :param match_glob: (Optional) filters objects based on the glob pattern given by the string
            (e.g, ``'**/*/.json'``).
        :param user_project: The identifier of the Google Cloud project to bill for the request.
            Required for Requester Pays buckets.
        :return: a stream of object names matching the filtering criteria
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name, user_project=user_project)

        ids = []
        page_token = None
        while True:
            if match_glob:
                blobs = self._list_blobs_with_match_glob(
                    bucket=bucket,
                    client=client,
                    match_glob=match_glob,
                    max_results=max_results,
                    page_token=page_token,
                    path=bucket.path + "/o",
                    prefix=prefix,
                    versions=versions,
                )
            else:
                blobs = bucket.list_blobs(
                    max_results=max_results,
                    page_token=page_token,
                    prefix=prefix,
                    delimiter=delimiter,
                    versions=versions,
                )

            blob_names = [blob.name for blob in blobs]

            if blobs.prefixes:
                ids.extend(blobs.prefixes)
            else:
                ids.extend(blob_names)

            page_token = blobs.next_page_token
            if page_token is None:
                # empty next page token
                break
        return ids

    @staticmethod
    def _list_blobs_with_match_glob(
        bucket,
        client,
        path: str,
        max_results: int | None = None,
        page_token: str | None = None,
        match_glob: str | None = None,
        prefix: str | None = None,
        versions: bool | None = None,
    ) -> Any:
        """
        List blobs when match_glob param is given.

        This method is a patched version of google.cloud.storage Client.list_blobs().
        It is used as a temporary workaround to support "match_glob" param,
        as it isn't officially supported by GCS Python client.
        (follow `issue #1035<https://github.com/googleapis/python-storage/issues/1035>`__).
        """
        from google.api_core import page_iterator
        from google.cloud.storage.bucket import _blobs_page_start, _item_to_blob

        extra_params: Any = {}
        if prefix is not None:
            extra_params["prefix"] = prefix
        if match_glob is not None:
            extra_params["matchGlob"] = match_glob
        if versions is not None:
            extra_params["versions"] = versions
        api_request = functools.partial(
            client._connection.api_request, timeout=DEFAULT_TIMEOUT, retry=DEFAULT_RETRY
        )

        blobs: Any = page_iterator.HTTPIterator(
            client=client,
            api_request=api_request,
            path=path,
            item_to_value=_item_to_blob,
            page_token=page_token,
            max_results=max_results,
            extra_params=extra_params,
            page_start=_blobs_page_start,
        )
        blobs.prefixes = set()
        blobs.bucket = bucket
        return blobs

    def list_by_timespan(
        self,
        bucket_name: str,
        timespan_start: datetime,
        timespan_end: datetime,
        versions: bool | None = None,
        max_results: int | None = None,
        prefix: str | None = None,
        delimiter: str | None = None,
        match_glob: str | None = None,
    ) -> List[str]:
        """
        List all objects from the bucket with the given string prefix that were updated in the time range.

        :param bucket_name: bucket name
        :param timespan_start: will return objects that were updated at or after this datetime (UTC)
        :param timespan_end: will return objects that were updated before this datetime (UTC)
        :param versions: if true, list all versions of the objects
        :param max_results: max count of items to return in a single page of responses
        :param prefix: prefix string which filters objects whose name begin with
            this prefix
        :param delimiter: (Deprecated) filters objects based on the delimiter (for e.g '.csv')
        :param match_glob: (Optional) filters objects based on the glob pattern given by the string
            (e.g, ``'**/*/.json'``).
        :return: a stream of object names matching the filtering criteria
        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)

        ids = []
        page_token = None

        while True:
            if match_glob:
                blobs = self._list_blobs_with_match_glob(
                    bucket=bucket,
                    client=client,
                    match_glob=match_glob,
                    max_results=max_results,
                    page_token=page_token,
                    path=bucket.path + "/o",
                    prefix=prefix,
                    versions=versions,
                )
            else:
                blobs = bucket.list_blobs(
                    max_results=max_results,
                    page_token=page_token,
                    prefix=prefix,
                    delimiter=delimiter,
                    versions=versions,
                )

            blob_names = [
                blob.name
                for blob in blobs
                if timespan_start <= blob.updated.replace(tzinfo=timezone.utc) < timespan_end
            ]

            if blobs.prefixes:
                ids.extend(blobs.prefixes)
            else:
                ids.extend(blob_names)

            page_token = blobs.next_page_token
            if page_token is None:
                # empty next page token
                break
        return ids

    def _get_blob(self, bucket_name: str, object_name: str) -> Blob:
        """
        Get a blob object in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :param object_name: The name of the object to check in the Google
            cloud storage bucket_name.

        """
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name=object_name)

        if blob is None:
            raise ValueError(f"Object ({object_name}) not found in Bucket ({bucket_name})")

        return blob

    def get_size(self, bucket_name: str, object_name: str) -> int:
        """
        Get the size of a file in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :param object_name: The name of the object to check in the Google
            cloud storage bucket_name.

        """
        self.log.info("Checking the file size of object: %s in bucket_name: %s", object_name, bucket_name)
        blob = self._get_blob(bucket_name, object_name)
        blob_size = blob.size
        self.log.info("The file size of %s is %s bytes.", object_name, blob_size)
        return blob_size

    def get_crc32c(self, bucket_name: str, object_name: str):
        """
        Get the CRC32c checksum of an object in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :param object_name: The name of the object to check in the Google cloud
            storage bucket_name.
        """
        self.log.info(
            "Retrieving the crc32c checksum of object_name: %s in bucket_name: %s",
            object_name,
            bucket_name,
        )
        blob = self._get_blob(bucket_name, object_name)
        blob_crc32c = blob.crc32c
        self.log.info("The crc32c checksum of %s is %s", object_name, blob_crc32c)
        return blob_crc32c

    def get_md5hash(self, bucket_name: str, object_name: str) -> str:
        """
        Get the MD5 hash of an object in Google Cloud Storage.

        :param bucket_name: The Google Cloud Storage bucket where the blob_name is.
        :param object_name: The name of the object to check in the Google cloud
            storage bucket_name.
        """
        self.log.info("Retrieving the MD5 hash of object: %s in bucket: %s", object_name, bucket_name)
        blob = self._get_blob(bucket_name, object_name)
        blob_md5hash = blob.md5_hash
        self.log.info("The md5Hash of %s is %s", object_name, blob_md5hash)
        return blob_md5hash

    def get_metadata(self, bucket_name: str, object_name: str) -> dict | None:
        """
        Get the metadata of an object in Google Cloud Storage.

        :param bucket_name: Name of the Google Cloud Storage bucket where the object is.
        :param object_name: The name of the object containing the desired metadata
        :return: The metadata associated with the object
        """
        self.log.info("Retrieving the metadata dict of object (%s) in bucket (%s)", object_name, bucket_name)
        blob = self._get_blob(bucket_name, object_name)
        blob_metadata = blob.metadata
        if blob_metadata:
            self.log.info("Retrieved metadata of object (%s) with %s fields", object_name, len(blob_metadata))
        else:
            self.log.info("Metadata of object (%s) is empty or it does not exist", object_name)
        return blob_metadata

    @GoogleBaseHook.fallback_to_default_project_id
    def create_bucket(
        self,
        bucket_name: str,
        resource: dict | None = None,
        storage_class: str = "MULTI_REGIONAL",
        location: str = "US",
        project_id: str = PROVIDE_PROJECT_ID,
        labels: dict | None = None,
    ) -> str:
        """
        Create a new bucket.

        Google Cloud Storage uses a flat namespace, so you can't
        create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

        :param bucket_name: The name of the bucket.
        :param resource: An optional dict with parameters for creating the bucket.
            For information on available parameters, see Cloud Storage API doc:
            https://cloud.google.com/storage/docs/json_api/v1/buckets/insert
        :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage. Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.

            If this value is not specified when the bucket is
            created, it will default to STANDARD.
        :param location: The location of the bucket.
            Object data for objects in the bucket resides in physical storage
            within this region. Defaults to US.

            .. seealso::
                https://developers.google.com/storage/docs/bucket-locations

        :param project_id: The ID of the Google Cloud Project.
        :param labels: User-provided labels, in key/value pairs.
        :return: If successful, it returns the ``id`` of the bucket.
        """
        self.log.info(
            "Creating Bucket: %s; Location: %s; Storage Class: %s", bucket_name, location, storage_class
        )

        # Add airflow-version label to the bucket
        labels = labels or {}
        labels["airflow-version"] = "v" + version.replace(".", "-").replace("+", "-")

        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        bucket_resource = resource or {}

        for item in bucket_resource:
            if item != "name":
                bucket._patch_property(name=item, value=resource[item])  # type: ignore[index]

        bucket.storage_class = storage_class
        bucket.labels = labels
        bucket.create(project=project_id, location=location)
        return bucket.id

    def insert_bucket_acl(
        self, bucket_name: str, entity: str, role: str, user_project: str | None = None
    ) -> None:
        """
        Create a new ACL entry on the specified bucket_name.

        See: https://cloud.google.com/storage/docs/json_api/v1/bucketAccessControls/insert

        :param bucket_name: Name of a bucket_name.
        :param entity: The entity holding the permission, in one of the following forms:
            user-userId, user-email, group-groupId, group-email, domain-domain,
            project-team-projectId, allUsers, allAuthenticatedUsers.
            See: https://cloud.google.com/storage/docs/access-control/lists#scopes
        :param role: The access permission for the entity.
            Acceptable values are: "OWNER", "READER", "WRITER".
        :param user_project: (Optional) The project to be billed for this request.
            Required for Requester Pays buckets.
        """
        self.log.info("Creating a new ACL entry in bucket: %s", bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        bucket.acl.reload()
        bucket.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            bucket.acl.user_project = user_project
        bucket.acl.save()

        self.log.info("A new ACL entry created in bucket: %s", bucket_name)

    def insert_object_acl(
        self,
        bucket_name: str,
        object_name: str,
        entity: str,
        role: str,
        generation: int | None = None,
        user_project: str | None = None,
    ) -> None:
        """
        Create a new ACL entry on the specified object.

        See: https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/insert

        :param bucket_name: Name of a bucket_name.
        :param object_name: Name of the object. For information about how to URL encode
            object names to be path safe, see:
            https://cloud.google.com/storage/docs/json_api/#encoding
        :param entity: The entity holding the permission, in one of the following forms:
            user-userId, user-email, group-groupId, group-email, domain-domain,
            project-team-projectId, allUsers, allAuthenticatedUsers
            See: https://cloud.google.com/storage/docs/access-control/lists#scopes
        :param role: The access permission for the entity.
            Acceptable values are: "OWNER", "READER".
        :param generation: Optional. If present, selects a specific revision of this object.
        :param user_project: (Optional) The project to be billed for this request.
            Required for Requester Pays buckets.
        """
        self.log.info("Creating a new ACL entry for object: %s in bucket: %s", object_name, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name=bucket_name)
        blob = bucket.blob(blob_name=object_name, generation=generation)
        # Reload fetches the current ACL from Cloud Storage.
        blob.acl.reload()
        blob.acl.entity_from_dict(entity_dict={"entity": entity, "role": role})
        if user_project:
            blob.acl.user_project = user_project
        blob.acl.save()

        self.log.info("A new ACL entry created for object: %s in bucket: %s", object_name, bucket_name)

    def compose(self, bucket_name: str, source_objects: List[str], destination_object: str) -> None:
        """
        Composes a list of existing object into a new object in the same storage bucket_name.

        Currently it only supports up to 32 objects that can be concatenated
        in a single operation

        https://cloud.google.com/storage/docs/json_api/v1/objects/compose

        :param bucket_name: The name of the bucket containing the source objects.
            This is also the same bucket to store the composed destination object.
        :param source_objects: The list of source objects that will be composed
            into a single object.
        :param destination_object: The path of the object if given.
        """
        if not source_objects:
            raise ValueError("source_objects cannot be empty.")

        if not bucket_name or not destination_object:
            raise ValueError("bucket_name and destination_object cannot be empty.")

        self.log.info("Composing %s to %s in the bucket %s", source_objects, destination_object, bucket_name)
        client = self.get_conn()
        bucket = client.bucket(bucket_name)
        destination_blob = bucket.blob(destination_object)
        destination_blob.compose(
            sources=[bucket.blob(blob_name=source_object) for source_object in source_objects]
        )

        self.log.info("Completed successfully.")

    def sync(
        self,
        source_bucket: str,
        destination_bucket: str,
        source_object: str | None = None,
        destination_object: str | None = None,
        recursive: bool = True,
        allow_overwrite: bool = False,
        delete_extra_files: bool = False,
    ) -> None:
        """
        Synchronize the contents of the buckets.

        Parameters ``source_object`` and ``destination_object`` describe the root sync directories. If they
        are not passed, the entire bucket will be synchronized. If they are passed, they should point
        to directories.

        .. note::
            The synchronization of individual files is not supported. Only entire directories can be
            synchronized.

        :param source_bucket: The name of the bucket containing the source objects.
        :param destination_bucket: The name of the bucket containing the destination objects.
        :param source_object: The root sync directory in the source bucket.
        :param destination_object: The root sync directory in the destination bucket.
        :param recursive: If True, subdirectories will be considered
        :param recursive: If True, subdirectories will be considered
        :param allow_overwrite: if True, the files will be overwritten if a mismatched file is found.
            By default, overwriting files is not allowed
        :param delete_extra_files: if True, deletes additional files from the source that not found in the
            destination. By default extra files are not deleted.

            .. note::
                This option can delete data quickly if you specify the wrong source/destination combination.

        :return: none
        """
        client = self.get_conn()

        # Create bucket object
        source_bucket_obj = client.bucket(source_bucket)
        destination_bucket_obj = client.bucket(destination_bucket)

        # Normalize parameters when they are passed
        source_object = normalize_directory_path(source_object)
        destination_object = normalize_directory_path(destination_object)

        # Calculate the number of characters that remove from the name, because they contain information
        # about the parent's path
        source_object_prefix_len = len(source_object) if source_object else 0

        # Prepare synchronization plan
        to_copy_blobs, to_delete_blobs, to_rewrite_blobs = self._prepare_sync_plan(
            source_bucket=source_bucket_obj,
            destination_bucket=destination_bucket_obj,
            source_object=source_object,
            destination_object=destination_object,
            recursive=recursive,
        )
        self.log.info(
            "Planned synchronization. To delete blobs count: %s, to upload blobs count: %s, "
            "to rewrite blobs count: %s",
            len(to_delete_blobs),
            len(to_copy_blobs),
            len(to_rewrite_blobs),
        )

        # Copy missing object to new bucket
        if not to_copy_blobs:
            self.log.info("Skipped blobs copying.")
        else:
            for blob in to_copy_blobs:
                dst_object = self._calculate_sync_destination_path(
                    blob, destination_object, source_object_prefix_len
                )
                self.rewrite(
                    source_bucket=source_bucket_obj.name,
                    source_object=blob.name,
                    destination_bucket=destination_bucket_obj.name,
                    destination_object=dst_object,
                )
            self.log.info("Blobs copied.")

        # Delete redundant files
        if not to_delete_blobs:
            self.log.info("Skipped blobs deleting.")
        elif delete_extra_files:
            # TODO: Add batch. I tried to do it, but the Google library is not stable at the moment.
            for blob in to_delete_blobs:
                self.delete(blob.bucket.name, blob.name)
            self.log.info("Blobs deleted.")

        # Overwrite files that are different
        if not to_rewrite_blobs:
            self.log.info("Skipped blobs overwriting.")
        elif allow_overwrite:
            for blob in to_rewrite_blobs:
                dst_object = self._calculate_sync_destination_path(
                    blob, destination_object, source_object_prefix_len
                )
                self.rewrite(
                    source_bucket=source_bucket_obj.name,
                    source_object=blob.name,
                    destination_bucket=destination_bucket_obj.name,
                    destination_object=dst_object,
                )
            self.log.info("Blobs rewritten.")

        self.log.info("Synchronization finished.")

    def _calculate_sync_destination_path(
        self, blob: storage.Blob, destination_object: str | None, source_object_prefix_len: int
    ) -> str:
        return (
            os.path.join(destination_object, blob.name[source_object_prefix_len:])
            if destination_object
            else blob.name[source_object_prefix_len:]
        )

    @staticmethod
    def _prepare_sync_plan(
        source_bucket: storage.Bucket,
        destination_bucket: storage.Bucket,
        source_object: str | None,
        destination_object: str | None,
        recursive: bool,
    ) -> tuple[set[storage.Blob], set[storage.Blob], set[storage.Blob]]:
        # Calculate the number of characters that are removed from the name, because they contain information
        # about the parent's path
        source_object_prefix_len = len(source_object) if source_object else 0
        destination_object_prefix_len = len(destination_object) if destination_object else 0
        delimiter = "/" if not recursive else None

        # Fetch blobs list
        source_blobs = list(source_bucket.list_blobs(prefix=source_object, delimiter=delimiter))
        destination_blobs = list(
            destination_bucket.list_blobs(prefix=destination_object, delimiter=delimiter)
        )

        # Create indexes that allow you to identify blobs based on their name
        source_names_index = {a.name[source_object_prefix_len:]: a for a in source_blobs}
        destination_names_index = {a.name[destination_object_prefix_len:]: a for a in destination_blobs}

        # Create sets with names without parent object name
        source_names = set(source_names_index.keys())
        # Discards empty string from source set that creates an empty subdirectory in
        # destination bucket with source subdirectory name
        source_names.discard("")
        destination_names = set(destination_names_index.keys())

        # Determine objects to copy and delete
        to_copy = source_names - destination_names
        to_delete = destination_names - source_names
        to_copy_blobs: set[storage.Blob] = {source_names_index[a] for a in to_copy}
        to_delete_blobs: set[storage.Blob] = {destination_names_index[a] for a in to_delete}

        # Find names that are in both buckets
        names_to_check = source_names.intersection(destination_names)
        to_rewrite_blobs: set[storage.Blob] = set()
        # Compare objects based on crc32
        for current_name in names_to_check:
            source_blob = source_names_index[current_name]
            destination_blob = destination_names_index[current_name]
            # If either object is CMEK-protected, use the Cloud Storage Objects Get API to retrieve them
            # so that the crc32c is included
            if source_blob.kms_key_name:
                source_blob = source_bucket.get_blob(source_blob.name, generation=source_blob.generation)
            if destination_blob.kms_key_name:
                destination_blob = destination_bucket.get_blob(
                    destination_blob.name, generation=destination_blob.generation
                )
            # if the objects are different, save it
            if source_blob.crc32c != destination_blob.crc32c:
                to_rewrite_blobs.add(source_blob)

        return to_copy_blobs, to_delete_blobs, to_rewrite_blobs


def gcs_object_is_directory(bucket: str) -> bool:
    """Return True if given Google Cloud Storage URL (gs://<bucket>/<blob>) is a directory or empty bucket."""
    _, blob = _parse_gcs_url(bucket)

    return len(blob) == 0 or blob.endswith("/")


def parse_json_from_gcs(
    gcp_conn_id: str,
    file_uri: str,
    impersonation_chain: str | Sequence[str] | None = None,
) -> Any:
    """
    Download and parses json file from Google cloud Storage.

    :param gcp_conn_id: Airflow Google Cloud connection ID.
    :param file_uri: full path to json file
        example: ``gs://test-bucket/dir1/dir2/file``
    """
    gcs_hook = GCSHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
    )
    bucket, blob = _parse_gcs_url(file_uri)
    with NamedTemporaryFile(mode="w+b") as file:
        try:
            gcs_hook.download(bucket_name=bucket, object_name=blob, filename=file.name)
        except GoogleAPICallError as ex:
            raise AirflowException(f"Failed to download file with query result: {ex}")

        file.seek(0)
        try:
            json_data = file.read()
        except (ValueError, OSError, RuntimeError) as ex:
            raise AirflowException(f"Failed to read file: {ex}")

        try:
            result = json.loads(json_data)
        except json.JSONDecodeError as ex:
            raise AirflowException(f"Failed to decode query result from bytes to json: {ex}")

        return result


def _parse_gcs_url(gsurl: str) -> tuple[str, str]:
    """
    Given a Google Cloud Storage URL, return a tuple containing the corresponding bucket and blob.

    Expected url format: gs://<bucket>/<blob>
    """
    parsed_url = urlsplit(gsurl)
    if not parsed_url.netloc:
        raise AirflowException("Please provide a bucket name")
    if parsed_url.scheme.lower() != "gs":
        raise AirflowException(f"Schema must be to 'gs://': Current schema: '{parsed_url.scheme}://'")

    bucket = parsed_url.netloc
    # Remove leading '/' but NOT trailing one
    blob = parsed_url.path.lstrip("/")
    return bucket, blob


class GCSAsyncHook(GoogleBaseAsyncHook):
    """GCSAsyncHook run on the trigger worker, inherits from GoogleBaseAsyncHook."""

    sync_hook_class = GCSHook

    async def get_storage_client(self, session: ClientSession) -> Storage:
        """Return a Google Cloud Storage service object."""
        token = await self.get_token(session=session)
        return Storage(
            token=token,
            session=cast(Session, session),
        )
