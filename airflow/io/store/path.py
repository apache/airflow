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
from io import UnsupportedOperation
from stat import S_ISLNK

from fsspec.utils import stringify_path

from airflow.io.store import ObjectStore, attach
from airflow.io.store.stat import stat_result

if typing.TYPE_CHECKING:
    from fsspec import AbstractFileSystem


def _rewrite_info(info: dict, store: ObjectStore) -> dict:
    info["name"] = ObjectStoragePath(info["name"], store=store)
    return info


@functools.total_ordering
class ObjectStoragePath(os.PathLike):
    """A path-like object for object storage."""

    __version__: typing.ClassVar[int] = 1

    sep: typing.ClassVar[str] = "/"
    root_marker: typing.ClassVar[str] = "/"

    _store: ObjectStore | None
    _bucket: str
    _key: str
    _conn_id: str | None
    _protocol: str
    _hash: int | None

    __slots__ = (
        "_store",
        "_bucket",
        "_key",
        "_conn_id",
        "_protocol",
        "_hash",
    )

    def __init__(
        self,
        path: str | ObjectStoragePath,
        *,
        conn_id: str | None = None,
        store: ObjectStore | None = None,
    ) -> None:
        self._conn_id = conn_id
        self._store = store

        if isinstance(path, ObjectStoragePath):
            self._protocol = path._protocol
            self._bucket = path._bucket
            self._key = path._key
            self._store = path._store
        else:
            self._protocol, self._bucket, self._key = self._split_path(path)

        if store:
            self._conn_id = store.conn_id
            self._protocol = self._protocol if self._protocol else store.protocol
        elif self._protocol and not self._store:
            self._store = attach(self._protocol, conn_id)

    @classmethod
    def _split_path(cls, p: typing.Any) -> tuple[str, str, str]:
        protocol, _, path = str(stringify_path(p)).rpartition("://")

        if cls.sep not in path:
            bucket = path
            key = ""
        else:
            bucket, key = path.split(cls.sep, 1)

        # we don't care about versions etc
        return protocol, bucket, key

    def __repr__(self) -> str:
        return f"<{type(self).__name__}('{self}')>"

    def __str__(self) -> str:
        return (
            f"{self._protocol}://{self._bucket}/{self._key}"
            if self._protocol
            else f"{self._bucket}/{self._key}"
        )

    __fspath__ = __str__

    def __lt__(self, other: typing.Any) -> bool:
        if not isinstance(other, ObjectStoragePath):
            return NotImplemented
        return self._bucket < other._bucket

    def __eq__(self, other: typing.Any) -> bool:
        if not isinstance(other, ObjectStoragePath):
            return NotImplemented
        return self._bucket == other._bucket

    def __ne__(self, other: typing.Any) -> bool:
        if not isinstance(other, ObjectStoragePath):
            return NotImplemented
        return self._bucket != other._bucket

    @functools.lru_cache
    def __hash__(self) -> int:
        return hash(self._bucket)

    def __truediv__(self, other: typing.Any) -> ObjectStoragePath:
        o_protocol, o_bucket, _ = self._split_path(other)
        if isinstance(other, ObjectStoragePath) and o_bucket and self._bucket != o_bucket:
            raise ValueError("Cannot combine paths from different buckets / containers")

        if o_protocol and self._protocol != o_protocol:
            raise ValueError("Cannot combine paths from different protocols")

        self_path = str(stringify_path(self))
        other_path = str(stringify_path(other))

        path = f"{self_path.rstrip(self.sep)}/{other_path.lstrip(self.sep)}"
        return ObjectStoragePath(path, conn_id=self._conn_id)

    def _unsupported(self, method_name: str) -> typing.NoReturn:
        msg = f"{type(self).__name__}.{method_name}() is unsupported"
        raise UnsupportedOperation(msg)

    def samestore(self, other: typing.Any) -> bool:
        return isinstance(other, ObjectStoragePath) and self._store == other._store

    @property
    def container(self) -> str:
        return self._bucket

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def key(self) -> str:
        return self._key

    @property
    def store(self) -> ObjectStore:
        if not self._store:
            raise ValueError("Cannot do operations. No store attached.")

        return self._store

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def fs(self) -> AbstractFileSystem:
        return self.store.fs

    @property
    def parent(self) -> ObjectStoragePath:
        return ObjectStoragePath(self.store.fs._parent(str(self)), store=self.store)

    def stat(self, *, follow_symlinks: bool = True) -> stat_result:
        """Return the result of the `stat()` call."""  # noqa: D402
        return stat_result(self.store.fs.stat(self), protocol=self.store.protocol, conn_id=self.store.conn_id)

    def lstat(self) -> stat_result:
        """Like stat() except that it doesn't follow symlinks."""
        return self.stat(follow_symlinks=False)

    def exists(self) -> bool:
        """Whether this path exists."""
        return self.store.fs.exists(self)

    def is_dir(self) -> bool:
        """Return True if this path is directory like."""
        return self.store.fs.isdir(self)

    def is_file(self) -> bool:
        """Return True if this path is a regular file."""
        return self.store.fs.isfile(self)

    def is_mount(self) -> bool:
        self._unsupported("is_mount")

    def is_symlink(self) -> bool:
        """Whether this path is a symbolic link."""
        try:
            return S_ISLNK(self.lstat().st_mode)
        except OSError:
            # Path doesn't exist
            return False
        except ValueError:
            # Non-encodable path
            return False

    def is_block_device(self) -> bool:
        self._unsupported("is_block_device")

    def is_char_device(self) -> bool:
        self._unsupported("is_char_device")

    def is_fifo(self) -> bool:
        self._unsupported("is_fifo")

    def is_socket(self) -> bool:
        self._unsupported("is_socket")

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

    def checksum(self) -> int:
        """Return the checksum of the file at this path."""
        return self.store.fs.checksum(self)

    def open(
        self,
        mode="rb",
        block_size=None,
        cache_options=None,
        compression=None,
        encoding=None,
        errors=None,
        newline=None,
        **kwargs,
    ) -> typing.IO:
        """
        Return a file-like object from the filesystem.

        The resultant instance must function correctly in a context 'with' block.

        :param mode: str like 'rb', 'w'
                  See builtin 'open()'.
        :param block_size: int
                        Some indication of buffering - this is a value in bytes.
        :param cache_options: dict, optional
                           Extra arguments to pass through to the cache.
        :param compression: string or None
                        If given, open file using a compression codec. Can either be a compression
                        name (a key in 'fsspec.compression.compr') or 'infer' to guess the
                        compression from the filename suffix.
        :param encoding: passed on to TextIOWrapper for text mode
        :param errors: passed on to TextIOWrapper for text mode
        :param newline: passed on to TextIOWrapper for text mode

        kwargs: Additional keyword arguments to be passed on.
        """
        return self.store.fs.open(
            str(self),
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            compression=compression,
            encoding=encoding,
            errors=errors,
            newline=newline,
            **kwargs,
        )

    def read_bytes(self, start: int | None = None, end: int | None = None) -> bytes:
        """Open the file in bytes mode, read it, and close the file."""
        return self.store.fs.read_bytes(str(self), start=start, end=end)

    def read_text(self, encoding=None, errors=None, newline=None, **kwargs) -> str:
        """Open the file in text mode, read it, and close the file."""
        return self.store.fs.read_text(str(self), encoding=encoding, errors=errors, newline=newline, **kwargs)

    def write_bytes(self, data, **kwargs) -> int:
        """Open the file in bytes mode, write to it, and close the file."""
        return self.store.fs.pipe_file(self, value=data, **kwargs)

    def write_text(self, data, encoding=None, errors=None, newline=None, **kwargs) -> int:
        """Open the file in text mode, write to it, and close the file."""
        return self.store.fs.write_text(
            str(self), value=data, encoding=encoding, errors=errors, newline=newline, **kwargs
        )

    def iterdir(self):
        """Iterate over the files in this directory."""
        self._unsupported("iterdir")

    def _scandir(self):
        # Emulate os.scandir(), which returns an object that can be used as a
        # context manager.
        return contextlib.nullcontext(self.iterdir())

    def glob(self, pattern: str, maxdepth: int | None = None, **kwargs):
        """
        Find files by glob-matching.

        If the path ends with '/', only folders are returned.

        We support ``"**"``,
        ``"?"`` and ``"[..]"``. We do not support ^ for pattern negation.

        The `maxdepth` option is applied on the first `**` found in the path.

        Search path names that contain embedded characters special to this
        implementation of glob may not produce expected results;
        e.g., 'foo/bar/*starredfilename*'.

        :param pattern: str
                       The glob pattern to match against.
        :param maxdepth: int or None
                         The maximum depth to search. If None, there is no depth limit.

        kwargs: Additional keyword arguments to be passed on.
        """
        path = os.path.join(self._bucket, pattern)

        detail = kwargs.get("detail", False)
        items = self.store.fs.glob(path, maxdepth=maxdepth, **kwargs)
        if detail:
            t = {
                ObjectStoragePath(k, store=self.store): _rewrite_info(v, self.store) for k, v in items.items()
            }
            return t
        else:
            return [ObjectStoragePath(c, store=self.store) for c in items]

    def rglob(self, maxdepth: int | None = None, **kwargs):
        self._unsupported("rglob")

    def walk(self, maxdepth: int | None = None, topdown: bool = True, on_error: str = "omit", **kwargs):
        """
        Return all files belows path.

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        When topdown is True, the caller can modify the dirnames list in-place (perhaps
        using del or slice assignment), and walk() will
        only recurse into the subdirectories whose names remain in dirnames;
        this can be used to prune the search, impose a specific order of visiting,
        or even to inform walk() about directories the caller creates or renames before
        it resumes walk() again.
        Modifying dirnames when topdown is False has no effect. (see os.walk)

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        :param maxdepth: int or None
                        Maximum recursion depth. None means limitless, but not recommended
                        on link-based file-systems.
        :param topdown: bool (True)
                        Whether to walk the directory tree from the top downwards or from
                        the bottom upwards.
        :param on_error: "omit", "raise", a collable
                        if omit (default), path with exception will simply be empty;
                        If raise, an underlying exception will be raised;
                        if callable, it will be called with a single OSError instance as argument
        kwargs: Additional keyword arguments to be passed on.
        """
        detail = kwargs.get("detail", False)
        items = self.store.fs.walk(str(self), maxdepth=maxdepth, topdown=topdown, on_error=on_error, **kwargs)
        if not detail:
            for path, dirs, files in items:
                yield ObjectStoragePath(path, store=self.store), dirs, files
        else:
            for path, dirs, files in items:
                yield (
                    ObjectStoragePath(path, store=self.store),
                    {k: _rewrite_info(v, self.store) for k, v in dirs.items()},
                    {k: _rewrite_info(v, self.store) for k, v in files.items()},
                )

    def ls(self, detail: bool = True, **kwargs) -> list[ObjectStoragePath] | list[dict]:
        """
        List files at path.

        :param detail: bool
                       If True, return a dict containing details about each entry, otherwise
                       return a list of paths.

        kwargs: Additional keyword arguments to be passed on.
        """
        items = self.store.fs.ls(str(self), detail=detail, **kwargs)

        if detail:
            return [_rewrite_info(c, self.store) for c in items]
        else:
            return [ObjectStoragePath(c, store=self.store) for c in items]

    def absolute(self) -> ObjectStoragePath:
        """Return an absolute version of this path. Resolving any aliases."""
        return ObjectStoragePath(f"{self.store.protocol}://{self._key}")

    def touch(self, truncate: bool = True) -> None:
        """Create an empty file, or update the timestamp.

        :param truncate: bool (True)
                         If True, always set the file size to 0; if False, update the timestamp and
                         leave the file unchanged, if the backend allows this.
        """
        self.store.fs.touch(str(self), truncate=truncate)

    def mkdir(self, create_parents: bool = True, exists_ok: bool = False, **kwargs) -> None:
        """
        Create a directory entry at the specified path or within a bucket/container.

        For systems that don't have true directories, it may create a directory entry
        for this instance only and not affect the real filesystem.

        :param create_parents: bool
                              if True, this is equivalent to 'makedirs'.
        :param exists_ok: bool
                            if True, do not raise an error if the target directory already exists.

        kwargs: Additional keyword arguments, which may include permissions, etc.
        """
        if not exists_ok and self.exists():
            raise FileExistsError(f"Target {self} exists")
        with contextlib.suppress(FileExistsError):
            self.store.fs.mkdir(str(self), create_parents=create_parents, **kwargs)

    def unlink(self, recursive: bool = False, maxdepth: int | None = None) -> None:
        """
        Remove this file or link.

        If the path is a directory, use rmdir() instead.
        """
        self.store.fs.rm(str(self), recursive=recursive, maxdepth=maxdepth)

    rm = unlink
    """
    Remove this file or link.

    Alias of unlink
    """

    def rmdir(self) -> None:
        """Remove this directory.  The directory must be empty."""
        self.store.fs.rmdir(str(self))

    def rename(self, target: str | ObjectStoragePath, overwrite=False) -> ObjectStoragePath:
        """
        Rename this path to the target path.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        if isinstance(target, str):
            target = ObjectStoragePath(target, store=self.store)

        if not self.samestore(target):
            raise ValueError("You can only rename within the same store")

        if not overwrite and self.store.fs.exists(target):
            raise FileExistsError(f"Target {target} exists")

        return ObjectStoragePath(self.store.fs.mv(str(self), target), store=self._store)

    def replace(self, target: str | ObjectStoragePath) -> ObjectStoragePath:
        """
        Rename this path to the target path, overwriting if that path exists.

        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.

        Returns the new Path instance pointing to the target path.
        """
        return self.rename(target, overwrite=True)

    # EXTENDED OPERATIONS

    def ukey(self) -> str:
        """Hash of file properties, to tell if it has changed."""
        return self.store.fs.ukey(str(self))

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
        return self.store.fs.read_block(str(self), offset, length, delimiter=delimiter)

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
        return self.store.fs.sign(str(self), expiration=expiration, **kwargs)

    def size(self) -> int:
        """Size in bytes of the file at this path."""
        return self.store.fs.size(self)

    def du(self, total: bool = True, maxdepth: int | None = None, withdirs: bool = False, **kwargs):
        """Space used by files and optionally directories within a path.

        Directory size does not include the size of its contents.

        :param total: bool
                     Whether to sum all the file sizes
        :param maxdepth: int or None
                         Maximum number of directory levels to descend, None for unlimited.
        :param withdirs: bool
                         Whether to include directory paths in the output.

        kwargs: Additional keyword arguments to be passed on.

        :returns: Dict of {path: size} if total=False, or int otherwise, where numbers
                  refer to bytes used.
        """
        return self.store.fs.du(str(self), total=total, maxdepth=maxdepth, withdirs=withdirs, **kwargs)

    def find(
        self, path: str, maxdepth: int | None = None, withdirs: bool = False, detail: bool = False, **kwargs
    ):
        """List all files below the specified path.

        Like posix ``find`` command without conditions.

        :param path: str
                     Path pattern to search.
        :param maxdepth: int or None
                         If not None, the maximum number of levels to descend.
        :param withdirs: bool
                         Whether to include directory paths in the output. This is True
                         when used by glob, but users usually only want files.
        :param detail: bool
                       Whether to include file info.

        kwargs: Additional keyword arguments to be passed to ``ls``.
        """
        path = self.sep.join([str(self), path.lstrip("/")])
        items = self.store.fs.find(path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs)

        if detail:
            return {
                ObjectStoragePath(k, store=self.store): _rewrite_info(v, self.store) for k, v in items.items()
            }
        else:
            return [ObjectStoragePath(c, store=self.store) for c in items]

    def _cp_file(self, dst: str | ObjectStoragePath, **kwargs):
        """Copy a single file from this path to another location by streaming the data."""
        if isinstance(dst, str):
            dst = ObjectStoragePath(dst)

        # create the directory or bucket if required
        if dst.key.endswith(self.sep) or not dst.key:
            dst.mkdir(exists_ok=True, create_parents=True)
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
            self.store.fs.copy(str(self), dst, recursive=recursive, **kwargs)
            return

        # use optimized path for local -> remote or remote -> local
        if self.store.protocol == "file":
            lpath = self.store.fs._strip_protocol(str(self))
            dst.store.fs.put(lpath, str(dst), recursive=recursive, **kwargs)
            return

        if dst.store.protocol == "file":
            rpath = dst.store.fs._strip_protocol(str(dst))
            self.store.fs.get(str(self), rpath, recursive=recursive, **kwargs)
            return

        if not self.exists():
            raise FileNotFoundError(f"{self} does not exist")

        # remote dir -> remote dir
        if self.is_dir():
            if dst.is_file():
                raise ValueError("Cannot copy directory to a file.")

            dst.mkdir(exists_ok=True, create_parents=True)

            out = self.store.fs.expand_path(str(self), recursive=True, **kwargs)
            source_stripped = self.store.fs._strip_protocol(str(self))

            for path in out:
                # this check prevents one extra call to is_dir() as
                # glob returns self as well
                if path == source_stripped:
                    continue

                src_obj = ObjectStoragePath(path, store=self.store)

                # skip directories, empty directories will not be created
                if src_obj.is_dir():
                    continue

                src_obj._cp_file(dst)

            return

        # remote file -> remote dir
        if self.is_file():
            self._cp_file(dst, **kwargs)
            return

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
            return self.store.fs.move(str(self), str(path), recursive=recursive, **kwargs)

        # non-local copy
        self.copy(path, recursive=recursive, **kwargs)
        self.unlink(recursive=recursive)

    def serialize(self) -> dict[str, str | ObjectStore]:
        return {
            "path": str(self),
            "store": self.store,
        }

    @classmethod
    def deserialize(cls, data: dict, version: int) -> ObjectStoragePath:
        if version > cls.__version__:
            raise ValueError(f"Cannot deserialize version {version} with version {cls.__version__}.")

        return ObjectStoragePath(**data)
