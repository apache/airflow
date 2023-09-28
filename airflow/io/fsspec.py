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
"""FileIO implementation for reading and writing table files that uses fsspec compatible filesystems."""
import errno
import json
import logging
import os
from functools import lru_cache, partial
from typing import (
    Any,
    Callable,
    Dict,
    Union,
)
from urllib.parse import urlparse

import requests

import requests
from botocore import UNSIGNED
from botocore.awsrequest import AWSRequest
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from requests import HTTPError

# from pyiceberg.catalog import TOKEN
# from pyiceberg.exceptions import SignError
from airflow.io import (
    IO_KEY,
    GCS_ACCESS,
    GCS_CACHE_TIMEOUT,
    GCS_CONSISTENCY,
    GCS_DEFAULT_LOCATION,
    GCS_ENDPOINT,
    GCS_PROJECT_ID,
    GCS_REQUESTER_PAYS,
    GCS_SESSION_KWARGS,
    GCS_TOKEN,
    GCS_VERSION_AWARE,
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_PROXY_URI,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
    FileIO,
    InputFile,
    InputStream,
    OutputFile,
    OutputStream,
    Properties,
    TOKEN,
)
from airflow.configuration import conf
from airflow.io.exceptions import SignError

log = logging.getLogger(__name__)


def s3v4_rest_signer(properties: Properties, request: AWSRequest, **_: Any) -> AWSRequest:
    if TOKEN not in properties:
        raise SignError("Signer set, but token is not available")

    signer_url = properties["uri"].rstrip("/")
    signer_headers = {"Authorization": f"Bearer {properties[TOKEN]}"}
    signer_body = {
        "method": request.method,
        "region": request.context["client_region"],
        "uri": request.url,
        "headers": {key: [val] for key, val in request.headers.items()},
    }

    response = requests.post(f"{signer_url}/v1/aws/s3/sign", headers=signer_headers, json=signer_body)
    try:
        response.raise_for_status()
        response_json = response.json()
    except HTTPError as e:
        raise SignError(f"Failed to sign request {response.status_code}: {signer_body}") from e

    for key, value in response_json["headers"].items():
        request.headers.add_header(key, ", ".join(value))

    request.url = response_json["uri"]

    return request


SIGNERS: Dict[str, Callable[[Properties, AWSRequest], AWSRequest]] = {"S3V4RestSigner": s3v4_rest_signer}


def _file(_: Properties) -> LocalFileSystem:
    return LocalFileSystem()


def _s3(properties: Properties) -> AbstractFileSystem:
    from s3fs import S3FileSystem

    client_kwargs = {
        "endpoint_url": properties.get(S3_ENDPOINT),
        "aws_access_key_id": properties.get(S3_ACCESS_KEY_ID),
        "aws_secret_access_key": properties.get(S3_SECRET_ACCESS_KEY),
        "aws_session_token": properties.get(S3_SESSION_TOKEN),
        "region_name": properties.get(S3_REGION),
    }
    config_kwargs = {}
    register_events: Dict[str, Callable[[Properties], None]] = {}

    if signer := properties.get("s3.signer"):
        log.info("Loading signer %s", signer)
        if singer_func := SIGNERS.get(signer):
            singer_func_with_properties = partial(singer_func, properties)
            register_events["before-sign.s3"] = singer_func_with_properties

            # Disable the AWS Signer
            config_kwargs["signature_version"] = UNSIGNED
        else:
            raise ValueError(f"Signer not available: {signer}")

    if proxy_uri := properties.get(S3_PROXY_URI):
        config_kwargs["proxies"] = {"http": proxy_uri, "https": proxy_uri}

    fs = S3FileSystem(client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    for event_name, event_function in register_events.items():
        fs.s3.meta.events.register_last(event_name, event_function, unique_id=1925)

    return fs


def _gs(properties: Properties) -> AbstractFileSystem:
    # https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem
    from gcsfs import GCSFileSystem

    return GCSFileSystem(
        project=properties.get(IO_KEY, GCS_PROJECT_ID),
        access=properties.get(GCS_ACCESS, "full_control"),
        token=properties.get(GCS_TOKEN),
        consistency=properties.get(GCS_CONSISTENCY, "none"),
        cache_timeout=properties.get(GCS_CACHE_TIMEOUT),
        requester_pays=properties.get(GCS_REQUESTER_PAYS, False),
        session_kwargs=json.loads(properties.get(GCS_SESSION_KWARGS, "{}")),
        endpoint_url=properties.get(GCS_ENDPOINT),
        default_location=properties.get(GCS_DEFAULT_LOCATION),
        version_aware=properties.get(GCS_VERSION_AWARE, "false").lower() == "true",
    )


def _adlfs(properties: Properties) -> AbstractFileSystem:
    from adlfs import AzureBlobFileSystem

    return AzureBlobFileSystem(
        connection_string=properties.get("adlfs.connection-string"),
        account_name=properties.get("adlfs.account-name"),
        account_key=properties.get("adlfs.account-key"),
        sas_token=properties.get("adlfs.sas-token"),
        tenant_id=properties.get("adlfs.tenant-id"),
        client_id=properties.get("adlfs.client-id"),
        client_secret=properties.get("adlfs.client-secret"),
    )


SCHEME_TO_FS = {
    "file": _file,
    "s3": _s3,
    "s3a": _s3,
    "s3n": _s3,
    "abfs": _adlfs,
    "abfss": _adlfs,
    "gs": _gs,
    "gcs": _gs,
}


class FsspecInputFile(InputFile):
    """An input file implementation for the FsspecFileIO.

    Args:
        location (str): A URI to a file location.
        fs (AbstractFileSystem): An fsspec filesystem instance.
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        object_info = self._fs.info(self.location)
        if size := object_info.get("Size"):
            return size
        elif size := object_info.get("size"):
            return size
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Check whether the location exists."""
        return self._fs.lexists(self.location)

    def open(self, seekable: bool = True) -> InputStream:
        """Create an input stream for reading the contents of the file.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            OpenFile: An fsspec compliant file-like object.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        try:
            return self._fs.open(self.location, "rb")
        except FileNotFoundError as e:
            # To have a consistent error handling experience, make sure exception contains missing file location.
            raise e if e.filename else FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), self.location
            ) from e


class FsspecOutputFile(OutputFile):
    """An output file implementation for the FsspecFileIO.

    Args:
        location (str): A URI to a file location.
        fs (AbstractFileSystem): An fsspec filesystem instance.
    """

    def __init__(self, location: str, fs: AbstractFileSystem):
        self._fs = fs
        super().__init__(location=location)

    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""
        object_info = self._fs.info(self.location)
        if size := object_info.get("Size"):
            return size
        elif size := object_info.get("size"):
            return size
        raise RuntimeError(f"Cannot retrieve object info: {self.location}")

    def exists(self) -> bool:
        """Check whether the location exists."""
        return self._fs.lexists(self.location)

    def create(self, overwrite: bool = False) -> OutputStream:
        """Create an output stream for reading the contents of the file.

        Args:
            overwrite (bool): Whether to overwrite the file if it already exists.

        Returns:
            OpenFile: An fsspec compliant file-like object.

        Raises:
            FileExistsError: If the file already exists at the location and overwrite is set to False.

        Note:
            If overwrite is set to False, a check is first performed to verify that the file does not exist.
            This is not thread-safe and a possibility does exist that the file can be created by a concurrent
            process after the existence check yet before the output stream is created. In such a case, the default
            behavior will truncate the contents of the existing file when opening the output stream.
        """
        if not overwrite and self.exists():
            raise FileExistsError(f"Cannot create file, file already exists: {self.location}")
        return self._fs.open(self.location, "wb")

    def to_input_file(self) -> FsspecInputFile:
        """Return a new FsspecInputFile for the location at `self.location`."""
        return FsspecInputFile(location=self.location, fs=self._fs)


class FsspecFileIO(FileIO):
    """A FileIO implementation that uses fsspec."""

    def __init__(self):
        self._scheme_to_fs = {}
        self._scheme_to_fs.update(SCHEME_TO_FS)
        self.get_fs: Callable[[str], AbstractFileSystem] = lru_cache(self._get_fs)
        super().__init__()

    def new_input(self, location: str, conn_id: str | None) -> FsspecInputFile:
        """Get an FsspecInputFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
            conn_id (str, optional): The connection ID to use for the file. Defaults
            to using the default connection.

        Returns:
            FsspecInputFile: An FsspecInputFile instance for the given location.
        """
        uri = urlparse(location)
        fs = self.get_fs(uri.scheme)
        return FsspecInputFile(location=location, fs=fs)

    def new_output(self, location: str, conn_id: str | None) -> FsspecOutputFile:
        """Get an FsspecOutputFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
            conn_id (str, optional): The connection ID to use for the file. Defaults
            to using the default connection.
        Returns:
            FsspecOutputFile: An FsspecOutputFile instance for the given location.
        """
        uri = urlparse(location)
        fs = self.get_fs(uri.scheme)
        return FsspecOutputFile(location=location, fs=fs)

    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given location.

        Args:
            location (Union[str, InputFile, OutputFile]): The URI to the file--if an InputFile instance or an
                OutputFile instance is provided, the location attribute for that instance is used as the location
                to delete.
        """
        if isinstance(location, (InputFile, OutputFile)):
            str_location = location.location  # Use InputFile or OutputFile location
        else:
            str_location = location

        uri = urlparse(str_location)
        fs = self.get_fs(uri.scheme)
        fs.rm(str_location)

    def _get_fs(self, scheme: str) -> AbstractFileSystem:
        """Get a filesystem for a specific scheme."""
        if scheme not in self._scheme_to_fs:
            raise ValueError(f"No registered filesystem for scheme: {scheme}")
        return self._scheme_to_fs[scheme](self.properties)
