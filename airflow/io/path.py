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
import functools
import os
import shutil
import typing
from pathlib import PurePath
from urllib.parse import urlsplit

from fsspec.core import split_protocol
from fsspec.utils import stringify_path
from upath.implementations.cloud import CloudPath, _CloudAccessor
from upath.registry import get_upath_class

from airflow.io.store import attach
from airflow.io.utils.stat import stat_result

if typing.TYPE_CHECKING:
    from urllib.parse import SplitResult

    from fsspec import AbstractFileSystem


PT = typing.TypeVar("PT", bound="ObjectStoragePath")

default = "file"


class _AirflowCloudAccessor(_CloudAccessor):
    __slots__ = ("_store",)

    def __init__(
        self,
        parsed_url: SplitResult | None,
        conn_id: str | None = None,
        **kwargs: typing.Any,
    ) -> None:
        if parsed_url and parsed_url.scheme:
            self._store = attach(parsed_url.scheme, conn_id)
        else:
            self._store = attach("file", conn_id)

    @property
    def _fs(self) -> AbstractFileSystem:
        return self._store.fs

    def __eq__(self, other):
        return isinstance(other, _AirflowCloudAccessor) and self._store == other._store


class ObjectStoragePath(CloudPath):
    """A path-like object for object storage."""

    _accessor: _AirflowCloudAccessor

    __version__: typing.ClassVar[int] = 1

    _default_accessor = _AirflowCloudAccessor

    sep: typing.ClassVar[str] = "/"
    root_marker: typing.ClassVar[str] = "/"

    _bucket: str
    _key: str
    _protocol: str
    _hash: int | None

    __slots__ = (
        "_bucket",
        "_key",
        "_conn_id",
        "_protocol",
        "_hash",
    )

    def __new__(
        cls: type[PT],
        *args: str | os.PathLike,
        scheme: str | None = None,
        conn_id: str | None = None,
        **kwargs: typing.Any,
    ) -> PT:
        args_list = list(args)

        if args_list:
            other = args_list.pop(0) or "."
        else:
            other = "."

        if isinstance(other, PurePath):
            _cls: typing.Any = type(other)
            drv, root, parts = _cls._parse_args(args_list)
            drv, root, parts = _cls._flavour.join_parsed_parts(
                other._drv,  # type: ignore[attr-defined]
                other._root,  # type: ignore[attr-defined]
                other._parts,  # type: ignore[attr-defined]
                drv,
                root,
                parts,  # type: ignore
            )

            _kwargs = getattr(other, "_kwargs", {})
            _url = getattr(other, "_url", None)
            other_kwargs = _kwargs.copy()
            if _url and _url.scheme:
                other_kwargs["url"] = _url
            new_kwargs = _kwargs.copy()
            new_kwargs.update(kwargs)

            return _cls(_cls._format_parsed_parts(drv, root, parts, **other_kwargs), **new_kwargs)

        url = stringify_path(other)
        parsed_url: SplitResult = urlsplit(url)

        if scheme:  # allow override of protocol
            parsed_url = parsed_url._replace(scheme=scheme)

        if not parsed_url.path:  # ensure path has root
            parsed_url = parsed_url._replace(path="/")

        if not parsed_url.scheme and not split_protocol(url)[0]:
            args_list.insert(0, url)
        else:
            args_list.insert(0, parsed_url.path)

        # This matches the parsing logic in urllib.parse; see:
        # https://github.com/python/cpython/blob/46adf6b701c440e047abf925df9a75a/Lib/urllib/parse.py#L194-L203
        userinfo, have_info, hostinfo = parsed_url.netloc.rpartition("@")
        if have_info:
            conn_id = conn_id or userinfo or None
            parsed_url = parsed_url._replace(netloc=hostinfo)

        return cls._from_parts(args_list, url=parsed_url, conn_id=conn_id, **kwargs)  # type: ignore

    @functools.lru_cache
    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: typing.Any) -> bool:
        return self.samestore(other) and str(self) == str(other)

    def samestore(self, other: typing.Any) -> bool:
        return isinstance(other, ObjectStoragePath) and self._accessor == other._accessor

    @property
    def container(self) -> str:
        return self.bucket

    @property
    def bucket(self) -> str:
        if self._url:
            return self._url.netloc
        else:
            return ""

    @property
    def key(self) -> str:
        if self._url:
            return self._url.path
        else:
            return ""

    def stat(self) -> stat_result:  # type: ignore[override]
        """Call ``stat`` and return the result."""
        return stat_result(
            self._accessor.stat(self),
            protocol=self.protocol,
            conn_id=self._accessor._store.conn_id,
        )

    def samefile(self, other_path: typing.Any) -> bool:
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
        return self.rename(target, overwrite=True)

    @classmethod
    def cwd(cls):
        if cls is ObjectStoragePath:
            return get_upath_class("").cwd()
        else:
            raise NotImplementedError

    @classmethod
    def home(cls):
        if cls is ObjectStoragePath:
            return get_upath_class("").home()
        else:
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
        r"""Read a block of bytes.

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
        >>> read_block(0, 13, delimiter=b'\\n')
        b'Alice, 100\\nBob, 200\\n'

        Use ``length=None`` to read to the end of the file.
        >>> read_block(0, None, delimiter=b'\\n')
        b'Alice, 100\\nBob, 200\\nCharlie, 300'

        See Also
        --------
        :func:`fsspec.utils.read_block`
        """
        return self.fs.read_block(self.path, offset=offset, length=length, delimiter=delimiter)

    def sign(self, expiration: int = 100, **kwargs):
        """Create a signed URL representing the given path.

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
        """Copy file(s) from this path to another location.

        For remote to remote copies, the key used for the destination will be the same as the source.
        So that s3://src_bucket/foo/bar will be copied to gcs://dst_bucket/foo/bar and not
        gcs://dst_bucket/bar.

        :param dst: Destination path
        :param recursive: If True, copy directories recursively.

        kwargs: Additional keyword arguments to be passed to the underlying implementation.
        """
        if isinstance(dst, str):
            dst = ObjectStoragePath(dst)

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

                src_obj = ObjectStoragePath(path, conn_id=self._accessor._store.conn_id)

                # skip directories, empty directories will not be created
                if src_obj.is_dir():
                    continue

                src_obj._cp_file(dst)

            return

        # remote file -> remote dir
        self._cp_file(dst, **kwargs)

    def move(self, path: str | ObjectStoragePath, recursive: bool = False, **kwargs) -> None:
        """Move file(s) from this path to another location.

        :param path: Destination path
        :param recursive: bool
                         If True, move directories recursively.

        kwargs: Additional keyword arguments to be passed to the underlying implementation.
        """
        if isinstance(path, str):
            path = ObjectStoragePath(path)

        if self.samestore(path):
            return self.fs.move(self.path, path.path, recursive=recursive, **kwargs)

        # non-local copy
        self.copy(path, recursive=recursive, **kwargs)
        self.unlink()

    def serialize(self) -> dict[str, typing.Any]:
        _kwargs = self._kwargs.copy()
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
