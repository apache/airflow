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

import contextlib
import os
import shutil
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import urlsplit

from fsspec.utils import stringify_path
from upath.implementations.cloud import CloudPath
from upath.registry import get_upath_class

from airflow.sdk.io.stat import stat_result
from airflow.sdk.io.store import attach

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


class _TrackingFileWrapper:
    """Wrapper that tracks file operations to intercept lineage."""

    def __init__(self, path: ObjectStoragePath, obj):
        super().__init__()
        self._path = path
        self._obj = obj

    def __getattr__(self, name):
        from airflow.lineage.hook import get_hook_lineage_collector

        if not callable(attr := getattr(self._obj, name)):
            return attr

        # If the attribute is a method, wrap it in another method to intercept the call
        def wrapper(*args, **kwargs):
            if name == "read":
                get_hook_lineage_collector().add_input_asset(context=self._path, uri=str(self._path))
            elif name == "write":
                get_hook_lineage_collector().add_output_asset(context=self._path, uri=str(self._path))
            result = attr(*args, **kwargs)
            return result

        return wrapper

    def __getitem__(self, key):
        # Intercept item access
        return self._obj[key]

    def __enter__(self):
        self._obj.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._obj.__exit__(exc_type, exc_val, exc_tb)


class ObjectStoragePath(CloudPath):
    """A path-like object for object storage."""

    __version__: ClassVar[int] = 1

    _protocol_dispatch = False

    sep: ClassVar[str] = "/"
    root_marker: ClassVar[str] = "/"

    __slots__ = ("_hash_cached",)

    @classmethod
    def _transform_init_args(
        cls,
        args: tuple[str | os.PathLike, ...],
        protocol: str,
        storage_options: dict[str, Any],
    ) -> tuple[tuple[str | os.PathLike, ...], str, dict[str, Any]]:
        """Extract conn_id from the URL and set it as a storage option."""
        if args:
            arg0 = args[0]
            parsed_url = urlsplit(stringify_path(arg0))
            userinfo, have_info, hostinfo = parsed_url.netloc.rpartition("@")
            if have_info:
                storage_options.setdefault("conn_id", userinfo or None)
                parsed_url = parsed_url._replace(netloc=hostinfo)
            args = (parsed_url.geturl(),) + args[1:]
            protocol = protocol or parsed_url.scheme
        return args, protocol, storage_options

    @classmethod
    def _fs_factory(
        cls, urlpath: str, protocol: str, storage_options: Mapping[str, Any]
    ) -> AbstractFileSystem:
        return attach(protocol or "file", storage_options.get("conn_id")).fs

    def __hash__(self) -> int:
        self._hash_cached: int
        try:
            return self._hash_cached
        except AttributeError:
            self._hash_cached = hash(str(self))
            return self._hash_cached

    def __eq__(self, other: Any) -> bool:
        return self.samestore(other) and str(self) == str(other)

    def samestore(self, other: Any) -> bool:
        return (
            isinstance(other, ObjectStoragePath)
            and self.protocol == other.protocol
            and self.storage_options.get("conn_id") == other.storage_options.get("conn_id")
        )

    @property
    def container(self) -> str:
        return self.bucket

    @property
    def bucket(self) -> str:
        if self._url:
            return self._url.netloc
        return ""

    @property
    def key(self) -> str:
        if self._url:
            # per convention, we strip the leading slashes to ensure a relative key is returned
            # we keep the trailing slash to allow for directory-like semantics
            return self._url.path.lstrip(self.sep)
        return ""

    @property
    def namespace(self) -> str:
        return f"{self.protocol}://{self.bucket}" if self.bucket else self.protocol

    def open(self, mode="r", **kwargs):
        """Open the file pointed to by this path."""
        kwargs.setdefault("block_size", kwargs.pop("buffering", None))
        return _TrackingFileWrapper(self, self.fs.open(self.path, mode=mode, **kwargs))

    def stat(self) -> stat_result:  # type: ignore[override]
        """Call ``stat`` and return the result."""
        return stat_result(
            self.fs.stat(self.path),
            protocol=self.protocol,
            conn_id=self.storage_options.get("conn_id"),
        )

    def samefile(self, other_path: Any) -> bool:
        """Return whether other_path is the same or not as this file."""
        if not isinstance(other_path, ObjectStoragePath):
            return False

        st = self.stat()
        other_st = other_path.stat()

        return (
            st["protocol"] == other_st["protocol"]
            and st["conn_id"] == other_st["conn_id"]
            and st["ino"] == other_st["ino"]
        )

    def _scandir(self):
        # Emulate os.scandir(), which returns an object that can be used as a
        # context manager.
        return contextlib.nullcontext(self.iterdir())

    def replace(self, target) -> ObjectStoragePath:
        """
        Rename this path to the target path, overwriting if that path exists.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        return self.rename(target)

    @classmethod
    def cwd(cls):
        if cls is ObjectStoragePath:
            return get_upath_class("").cwd()
        raise NotImplementedError

    @classmethod
    def home(cls):
        if cls is ObjectStoragePath:
            return get_upath_class("").home()
        raise NotImplementedError

    # EXTENDED OPERATIONS

    def ukey(self) -> str:
        """Hash of file properties, to tell if it has changed."""
        return self.fs.ukey(self.path)

    def checksum(self) -> int:
        """Return the checksum of the file at this path."""
        # we directly access the fs here to avoid changing the abstract interface
        return self.fs.checksum(self.path)

    def read_block(self, offset: int, length: int, delimiter=None):
        r"""
        Read a block of bytes.

        Starting at ``offset`` of the file, read ``length`` bytes. If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``. If ``offset`` is zero then we start at zero. The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        :param offset: int
                      Byte offset to start read
        :param length: int
                      Number of bytes to read. If None, read to the end.
        :param delimiter: bytes (optional)
                        Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> read_block(0, 13)
        b'Alice, 100\\nBo'
        >>> read_block(0, 13, delimiter=b"\\n")
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> read_block(0, None, delimiter=b"\\n")
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        :func:`fsspec.utils.read_block`
        """
        return self.fs.read_block(self.path, offset=offset, length=length, delimiter=delimiter)

    def sign(self, expiration: int = 100, **kwargs):
        """
        Create a signed URL representing the given path.

        Some implementations allow temporary URLs to be generated, as a
        way of delegating credentials.

        :param path: str
                     The path on the filesystem
        :param expiration: int
                          Number of seconds to enable the URL for (if supported)

        :returns URL: str
                     The signed URL

        :raises NotImplementedError: if the method is not implemented for a store
        """
        return self.fs.sign(self.path, expiration=expiration, **kwargs)

    def size(self) -> int:
        """Size in bytes of the file at this path."""
        return self.fs.size(self.path)

    def _cp_file(self, dst: ObjectStoragePath, **kwargs):
        """Copy a single file from this path to another location by streaming the data."""
        # create the directory or bucket if required
        if dst.key.endswith(self.sep) or not dst.key:
            dst.mkdir(exist_ok=True, parents=True)
            dst = dst / self.key
        elif dst.is_dir():
            dst = dst / self.key

        # streaming copy
        with self.open("rb") as f1, dst.open("wb") as f2:
            # make use of system dependent buffer size
            shutil.copyfileobj(f1, f2, **kwargs)

    def copy(self, dst: str | ObjectStoragePath, recursive: bool = False, **kwargs) -> None:
        """
        Copy file(s) from this path to another location.

        For remote to remote copies, the key used for the destination will be the same as the source.
        So that s3://src_bucket/foo/bar will be copied to gcs://dst_bucket/foo/bar and not
        gcs://dst_bucket/bar.

        :param dst: Destination path
        :param recursive: If True, copy directories recursively.

        kwargs: Additional keyword arguments to be passed to the underlying implementation.
        """
        from airflow.lineage.hook import get_hook_lineage_collector

        if isinstance(dst, str):
            dst = ObjectStoragePath(dst)

        if self.samestore(dst) or self.protocol == "file" or dst.protocol == "file":
            # only emit this in "optimized" variants - else lineage will be captured by file writes/reads
            get_hook_lineage_collector().add_input_asset(context=self, uri=str(self))
            get_hook_lineage_collector().add_output_asset(context=dst, uri=str(dst))

        # same -> same
        if self.samestore(dst):
            self.fs.copy(self.path, dst.path, recursive=recursive, **kwargs)
            return

        # use optimized path for local -> remote or remote -> local
        if self.protocol == "file":
            dst.fs.put(self.path, dst.path, recursive=recursive, **kwargs)
            return

        if dst.protocol == "file":
            self.fs.get(self.path, dst.path, recursive=recursive, **kwargs)
            return

        if not self.exists():
            raise FileNotFoundError(f"{self} does not exist")

        # remote dir -> remote dir
        if self.is_dir():
            if dst.is_file():
                raise ValueError("Cannot copy directory to a file.")

            dst.mkdir(exist_ok=True, parents=True)

            out = self.fs.expand_path(self.path, recursive=True, **kwargs)

            for path in out:
                # this check prevents one extra call to is_dir() as
                # glob returns self as well
                if path == self.path:
                    continue

                src_obj = ObjectStoragePath(
                    path,
                    protocol=self.protocol,
                    conn_id=self.storage_options.get("conn_id"),
                )

                # skip directories, empty directories will not be created
                if src_obj.is_dir():
                    continue

                src_obj._cp_file(dst)
            return

        # remote file -> remote dir
        self._cp_file(dst, **kwargs)

    def move(self, path: str | ObjectStoragePath, recursive: bool = False, **kwargs) -> None:
        """
        Move file(s) from this path to another location.

        :param path: Destination path
        :param recursive: bool
                         If True, move directories recursively.

        kwargs: Additional keyword arguments to be passed to the underlying implementation.
        """
        from airflow.lineage.hook import get_hook_lineage_collector

        if isinstance(path, str):
            path = ObjectStoragePath(path)

        if self.samestore(path):
            get_hook_lineage_collector().add_input_asset(context=self, uri=str(self))
            get_hook_lineage_collector().add_output_asset(context=path, uri=str(path))
            return self.fs.move(self.path, path.path, recursive=recursive, **kwargs)

        # non-local copy
        self.copy(path, recursive=recursive, **kwargs)
        self.unlink()

    def serialize(self) -> dict[str, Any]:
        _kwargs = {**self.storage_options}
        conn_id = _kwargs.pop("conn_id", None)

        return {
            "path": str(self),
            "conn_id": conn_id,
            "kwargs": _kwargs,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int) -> ObjectStoragePath:
        if version > cls.__version__:
            raise ValueError(f"Cannot deserialize version {version} with version {cls.__version__}.")

        _kwargs = data.pop("kwargs")
        path = data.pop("path")
        conn_id = data.pop("conn_id", None)

        return ObjectStoragePath(path, conn_id=conn_id, **_kwargs)

    def __str__(self):
        conn_id = self.storage_options.get("conn_id")
        if self._protocol and conn_id:
            return f"{self._protocol}://{conn_id}@{self.path}"
        return super().__str__()
