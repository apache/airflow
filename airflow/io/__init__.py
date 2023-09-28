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
"""Base FileIO classes for implementing reading and writing table files.

The FileIO abstraction includes a subset of full filesystem implementations. Specifically,
Iceberg needs to read or write a file at a given location (as a seekable stream), as well
as check if a file exists. An implementation of the FileIO abstract base class is responsible
for returning an InputFile instance, an OutputFile instance, and deleting a file given
its location.

Ported from Apache Iceberg.
"""
from __future__ import annotations

import importlib
import logging
import warnings
from abc import ABC, abstractmethod
from io import SEEK_SET
from types import TracebackType
from typing import (
    Dict,
    List,
    Optional,
    Protocol,
    Type,
    Union,
    runtime_checkable,
)
from urllib.parse import urlparse

log = logging.getLogger(__name__)

IO_KEY = "io"

S3_ENDPOINT = "s3.endpoint"
S3_ACCESS_KEY_ID = "s3.access-key-id"
S3_SECRET_ACCESS_KEY = "s3.secret-access-key"
S3_SESSION_TOKEN = "s3.session-token"
S3_REGION = "s3.region"
S3_PROXY_URI = "s3.proxy-uri"
HDFS_HOST = "hdfs.host"
HDFS_PORT = "hdfs.port"
HDFS_USER = "hdfs.user"
HDFS_KERB_TICKET = "hdfs.kerberos_ticket"
GCS_TOKEN = "gcs.oauth2.token"
GCS_TOKEN_EXPIRES_AT_MS = "gcs.oauth2.token-expires-at"
GCS_PROJECT_ID = "gcs.project-id"
GCS_ACCESS = "gcs.access"
GCS_CONSISTENCY = "gcs.consistency"
GCS_CACHE_TIMEOUT = "gcs.cache-timeout"
GCS_REQUESTER_PAYS = "gcs.requester-pays"
GCS_SESSION_KWARGS = "gcs.session-kwargs"
GCS_ENDPOINT = "gcs.endpoint"
GCS_DEFAULT_LOCATION = "gcs.default-bucket-location"
GCS_VERSION_AWARE = "gcs.version-aware"

TOKEN = "token"

Properties = Dict[str, str]


@runtime_checkable
class InputStream(Protocol):
    """A protocol for the file-like object returned by InputFile.open(...).

    This outlines the minimally required methods for a seekable input stream returned from an InputFile
    implementation's `open(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def read(self, size: int = 0) -> bytes:
        ...

    @abstractmethod
    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        ...

    @abstractmethod
    def tell(self) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    def __enter__(self) -> InputStream:
        """Provide setup when opening an InputStream using a 'with' statement."""

    @abstractmethod
    def __exit__(
        self,
        exctype: Optional[Type[BaseException]],
        excinst: Optional[BaseException],
        exctb: Optional[TracebackType],
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""


@runtime_checkable
class OutputStream(Protocol):  # pragma: no cover
    """A protocol for the file-like object returned by OutputFile.create(...).

    This outlines the minimally required methods for a writable output stream returned from an OutputFile
    implementation's `create(...)` method. These methods are a subset of IOBase/RawIOBase.
    """

    @abstractmethod
    def write(self, b: bytes) -> int:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> OutputStream:
        """Provide setup when opening an OutputStream using a 'with' statement."""

    @abstractmethod
    def __exit__(
        self,
        exctype: Optional[Type[BaseException]],
        excinst: Optional[BaseException],
        exctb: Optional[TracebackType],
    ) -> None:
        """Perform cleanup when exiting the scope of a 'with' statement."""


class InputFile(ABC):
    """A base class for InputFile implementations.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location (str): The URI or path to a local file for an InputFile instance.
        exists (bool): Whether the file exists or not.
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""

    @property
    def location(self) -> str:
        """The fully-qualified location of the input file."""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Check whether the location exists.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
        """

    @abstractmethod
    def open(self, seekable: bool = True) -> InputStream:
        """Return an object that matches the InputStream protocol.

        Args:
            seekable: If the stream should support seek, or if it is consumed sequential.

        Returns:
            InputStream: An object that matches the InputStream protocol.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
            FileNotFoundError: If the file at self.location does not exist.
        """


class OutputFile(ABC):
    """A base class for OutputFile implementations.

    Args:
        location (str): A URI or a path to a local file.

    Attributes:
        location (str): The URI or path to a local file for an OutputFile instance.
        exists (bool): Whether the file exists or not.
    """

    def __init__(self, location: str):
        self._location = location

    @abstractmethod
    def __len__(self) -> int:
        """Return the total length of the file, in bytes."""

    @property
    def location(self) -> str:
        """The fully-qualified location of the output file."""
        return self._location

    @abstractmethod
    def exists(self) -> bool:
        """Check whether the location exists.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
        """

    @abstractmethod
    def to_input_file(self) -> InputFile:
        """Return an InputFile for the location of this output file."""

    @abstractmethod
    def create(self, overwrite: bool = False) -> OutputStream:
        """Return an object that matches the OutputStream protocol.

        Args:
            overwrite (bool): If the file already exists at `self.location`
                and `overwrite` is False a FileExistsError should be raised.

        Returns:
            OutputStream: An object that matches the OutputStream protocol.

        Raises:
            PermissionError: If the file at self.location cannot be accessed due to a permission error.
            FileExistsError: If the file at self.location already exists and `overwrite=False`.
        """


class FileIO(ABC):
    """A base class for FileIO implementations."""

    @abstractmethod
    def new_input(self, location: str, conn_id: str | None) -> InputFile:
        """Get an InputFile instance to read bytes from the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
        """

    @abstractmethod
    def new_output(self, location: str, conn_id: str | None) -> OutputFile:
        """Get an OutputFile instance to write bytes to the file at the given location.

        Args:
            location (str): A URI or a path to a local file.
        """

    @abstractmethod
    def delete(self, location: Union[str, InputFile, OutputFile]) -> None:
        """Delete the file at the given path.

        Args:
            location (Union[str, InputFile, OutputFile]): A URI or a path to a local file--if an InputFile instance or
                an OutputFile instance is provided, the location attribute for that instance is used as the URI to delete.

        Raises:
            PermissionError: If the file at location cannot be accessed due to a permission error.
            FileNotFoundError: When the file at the provided location does not exist.
        """


FSSPEC_FILE_IO = "airflow.io.fsspec.FsspecFileIO"

# Mappings from the Java FileIO impl to a Python one. The list is ordered by preference.
# If an implementation isn't installed, it will fall back to the next one.
SCHEMA_TO_FILE_IO: Dict[str, List[str]] = {
    "s3": [FSSPEC_FILE_IO],
    "s3a": [FSSPEC_FILE_IO],
    "s3n": [FSSPEC_FILE_IO],
    # "gs": [ARROW_FILE_IO],
    # "file": [ARROW_FILE_IO],
    # "hdfs": [ARROW_FILE_IO],
    "abfs": [FSSPEC_FILE_IO],
    "abfss": [FSSPEC_FILE_IO],
}


def _import_file_io(io_impl: str) -> Optional[FileIO]:
    try:
        path_parts = io_impl.split(".")
        if len(path_parts) < 2:
            raise ValueError(f"py-io-impl should be full path (module.CustomFileIO), got: {io_impl}")
        module_name, class_name = ".".join(path_parts[:-1]), path_parts[-1]
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_()
    except ModuleNotFoundError:
        log.warning("Could not initialize FileIO: %s", io_impl)
        return None


PY_IO_IMPL = "py-io-impl"


def _infer_file_io_from_scheme(path: str) -> Optional[FileIO]:
    parsed_url = urlparse(path)
    if parsed_url.scheme:
        if file_ios := SCHEMA_TO_FILE_IO.get(parsed_url.scheme):
            for file_io_path in file_ios:
                if file_io := _import_file_io(file_io_path):
                    return file_io
        else:
            warnings.warn(f"No preferred file implementation for scheme: {parsed_url.scheme}")
    return None
