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

import functools
import os.path
from dataclasses import dataclass
from os import PathLike
from pathlib import PurePath, PurePosixPath
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

from fsspec.callbacks import NoOpCallback
from fsspec.utils import tokenize

from airflow.io.fsspec import SCHEME_TO_FS

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


@dataclass
class Mount(PathLike):
    """Manages a mount point for a filesystem or object storage."""

    source: str
    mount_point: str

    fs: AbstractFileSystem

    conn_id: str | None = None

    def wrap(self, method: str, *args, **kwargs):
        """
        Wrap a filesystem method to replace the mount point with the original source.

        :param method: the method to wrap
        :param args: the arguments to pass to the method
        :param kwargs: the keyword arguments to pass to the method
        :return: the result of the method
        :rtype: Any
        """
        path = kwargs.pop("path") if "path" in kwargs else args[0]
        path = self.replace_mount_point(cast(str, path))

        return getattr(self.fs, method)(path, *args[1:], **kwargs)

    def __str__(self):
        return self.mount_point

    def __fspath__(self):
        return self.__str__()

    def __enter__(self):
        return self.fs

    def __exit__(self, exc_type, exc_val, exc_tb):
        unmount(self.mount_point)

    def __truediv__(self, other):
        # if local we can run on nt or posix
        if self.fsid == "local":
            return PurePath(self.mount_point) / other

        # if remote we assume posix
        return PurePosixPath(self.mount_point) / other

    def __getattr__(self, item):
        return functools.partial(self.wrap, item)

    def replace_mount_point(self, path: str) -> str:
        new_path = path.replace(self.mount_point, self.source, 1).replace("//", "/")

        # check for traversal?
        if self.source not in new_path:
            new_path = os.path.join(self.source, new_path.lstrip(os.sep))

        return new_path

    @property
    def fsid(self) -> str:
        """
        Get the filesystem ID for this mount in order to be able to compare across instances.

        The underlying `fsid` is returned from the filesystem if available, otherwise it is generated
        from the protocol and connection ID.

        :return: deterministic the filesystem ID
        """
        try:
            return self.fs.fsid
        except NotImplementedError:
            return f"{self.fs.protocol}-{self.conn_id or 'env'}"


MOUNTS: dict[str, Mount] = {}


def get_mount(path: str) -> Mount:
    """
    Get the mount point for a given path.

    :param path: the path to get the mount point for
    :return: the mount point
    :rtype: Mount
    """
    mount_point = None
    mount_points = sorted(MOUNTS.keys(), key=len, reverse=True)

    for prefix in mount_points:
        if os.path.commonprefix([prefix, path]) == prefix:
            mount_point = prefix

    if mount_point is None:
        raise ValueError(f"No mount point found for path: {path}")

    mnt = MOUNTS.get(mount_point)
    if mnt is None:
        raise ValueError(f"Mount point {mount_point} not mounted")

    return mnt


def _replace_mount_point(path: str) -> str:
    """
    Replace the mount point in a path with the original source.

    :param path: the path to replace the mount point in
    :return: the path with the mount point replaced
    :rtype: str
    """
    mnt = get_mount(path)

    return path.replace(mnt.mount_point, mnt.source)


def _rewrite_path(path: str, mnt: Mount) -> str:
    """
    Rewrite a path to include the mount point and remove the original source.

    :param path: the path to rewrite
    :param mnt: the mount point to include in the path
    :return: the rewritten path
    :rtype: str
    """
    return os.path.join(mnt.mount_point, path.replace(mnt.source, "").lstrip(os.sep))


def _rewrite_info(info: dict, mnt: Mount) -> dict:
    """
    Rewrite the path in a file info dict to include the mount point and remove the original source.

    :param info: the file info dict to rewrite
    :param mnt: the mount point to include in the path
    :return: the rewritten file info dict
    :rtype: dict
    """
    info["original_name"] = info["name"]
    info["name"] = _rewrite_path(info["name"], mnt)

    return info


def mount(
    source: str,
    mount_point: str | None = None,
    conn_id: str | None = None,
    encryption_type: str | None = "",
    fs_type: AbstractFileSystem | None = None,
    remount: bool = False,
) -> Mount:
    """
    Mount a filesystem or object storage to a mount point.

    :param source: the source path to mount
    :param mount_point: the target mount point
    :param conn_id: the connection to use to connect to the filesystem
    :param encryption_type: the encryption type to use to connect to the filesystem
    :param fs_type: the filesystem type to use to connect to the filesystem
    :param remount: whether to remount the filesystem if it is already mounted
    """
    if not remount and mount_point and mount_point in MOUNTS:
        raise ValueError(f"Mount point {mount_point} already mounted")

    uri = urlparse(source)
    scheme = uri.scheme

    if not fs_type and scheme not in SCHEME_TO_FS:
        raise ValueError(f"No registered filesystem for scheme: {scheme}")

    if mount_point:
        try:
            other = get_mount(mount_point)
        except ValueError:
            other = None

        if other is not None and other.mount_point != mount_point:
            raise ValueError(f"Cannot nest {mount_point} into existing mount point {other.mount_point}")
    else:
        mount_point = cast(str, tokenize(source.lstrip("/")))

    fs = fs_type or SCHEME_TO_FS[scheme](conn_id)
    source = fs._strip_protocol(source)

    mnt = Mount(source=source, mount_point=mount_point, fs=fs)
    MOUNTS[mount_point] = mnt

    return mnt


def unmount(mount_point: str | Mount) -> None:
    """
    Unmount a filesystem or object storage from a mount point.

    :param mount_point: the mount point to unmount
    """
    if isinstance(mount_point, Mount):
        mount_point = mount_point.mount_point

    if mount_point not in MOUNTS:
        raise ValueError(f"Mount point {mount_point} not mounted")

    del MOUNTS[mount_point]


def get_fs(mount_point: str) -> AbstractFileSystem:
    """
    Get the filesystem for a given mount point or path.

    :param mount_point: the path to get the filesystem for
    :return: the filesystem
    :rtype: AbstractFileSystem
    """
    return get_mount(mount_point).fs


def mkdir(path, create_parents=True, **kwargs):
    """
    Create directory entry at path.

    For systems that don't have true directories, may create an for
    this instance only and not touch the real filesystem

    Parameters
    ----------
    path: str
        location
    create_parents: bool
        if True, this is equivalent to ``makedirs``
    kwargs:
        may be permissions, etc.
    """
    get_mount(path).mkdir(path, create_parents=create_parents, **kwargs)


def makedirs(path, exist_ok=False):
    """Recursively make directories.

    Creates directory at path and any intervening required directories.
    Raises exception if, for instance, the path already exists but is a
    file.

    Parameters
    ----------
    path: str
        leaf directory name
    exist_ok: bool (False)
        If False, will error if the target already exists
    """
    get_mount(path).makedirs(path, exist_ok=exist_ok)


def rmdir(path):
    """Remove a directory, if empty."""
    get_mount(path).rmdir(path)


def ls(path, detail=True, **kwargs):
    """List objects at path.

    This should include subdirectories and files at that location. The
    difference between a file and a directory must be clear when details
    are requested.

    The specific keys, or perhaps a FileInfo class, or similar, is TBD,
    but must be consistent across implementations.
    Must include:

    - full path to the entry (without protocol)
    - size of the entry, in bytes. If the value cannot be determined, will
      be ``None``.
    - type of entry, "file", "directory" or other

    Additional information
    may be present, appropriate to the file-system, e.g., generation,
    checksum, etc.

    May use refresh=True|False to allow use of self._ls_from_cache to
    check for a saved listing and avoid calling the backend. This would be
    common where listing may be expensive.

    Parameters
    ----------
    path: str
    detail: bool
        if True, gives a list of dictionaries, where each is the same as
        the result of ``info(path)``. If False, gives a list of paths
        (str).
    kwargs: may have additional backend-specific options, such as version
        information

    Returns
    -------
    List of strings if detail is False, or list of directory information
    dicts if detail is True.
    """
    mnt = get_mount(path)
    info = mnt.ls(path, detail=detail, **kwargs)

    if detail:
        for i, c in enumerate(info):
            _rewrite_info(c, mnt)
    else:
        for i, c in enumerate(info):
            info[i] = _rewrite_path(c, mnt)

    return info


def walk(path, maxdepth=None, topdown=True, on_error="omit", **kwargs):
    """Return all files belows path.

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

    Parameters
    ----------
    path: str
        Root to recurse into
    maxdepth: int
        Maximum recursion depth. None means limitless, but not recommended
        on link-based file-systems.
    topdown: bool (True)
        Whether to walk the directory tree from the top downwards or from
        the bottom upwards.
    on_error: "omit", "raise", a collable
        if omit (default), path with exception will simply be empty;
        If raise, an underlying exception will be raised;
        if callable, it will be called with a single OSError instance as argument
    kwargs: passed to ``ls``
    """
    mnt = get_mount(path)

    detail = kwargs.get("detail", False)

    if not detail:
        for path, dirs, files in mnt.walk(
            path,
            maxdepth=maxdepth,
            topdown=topdown,
            on_error=on_error,
            **kwargs,
        ):
            yield (
                _rewrite_path(path, mnt),
                dirs,
                files,
            )
    else:
        for path, dirs, files in mnt.walk(
            path,
            maxdepth=maxdepth,
            topdown=topdown,
            on_error=on_error,
            **kwargs,
        ):
            yield (
                _rewrite_path(path, mnt),
                {k: _rewrite_info(v, mnt) for k, v in dirs.items()},
                {k: _rewrite_info(v, mnt) for k, v in files.items()},
            )


def find(path, maxdepth=None, withdirs=False, detail=False, **kwargs):
    """List all files below path.

    Like posix ``find`` command without conditions

    Parameters
    ----------
    path : str
    maxdepth: int or None
        If not None, the maximum number of levels to descend
    withdirs: bool
        Whether to include directory paths in the output. This is True
        when used by glob, but users usually only want files.
    kwargs are passed to ``ls``.
    """
    mnt = get_mount(path)

    if not detail:
        names = []
        for name in mnt.find(
            path,
            maxdepth=maxdepth,
            withdirs=withdirs,
            detail=detail,
            **kwargs,
        ):
            names.append(_rewrite_path(name, mnt))
    else:
        items = mnt.find(path, maxdepth=maxdepth, withdirs=withdirs, detail=detail, **kwargs)
        names = {_rewrite_path(k, mnt): _rewrite_info(v, mnt) for k, v in items.items()}

    return names


def du(path, total=True, maxdepth=None, withdirs=False, **kwargs):
    """Space used by files and optionally directories within a path.

    Directory size does not include the size of its contents.

    Parameters
    ----------
    path: str
    total: bool
        Whether to sum all the file sizes
    maxdepth: int or None
        Maximum number of directory levels to descend, None for unlimited.
    withdirs: bool
        Whether to include directory paths in the output.
    kwargs: passed to ``find``

    Returns
    -------
    Dict of {path: size} if total=False, or int otherwise, where numbers
    refer to bytes used.
    """
    mnt = get_mount(path)
    return mnt.du(
        path,
        total=total,
        maxdepth=maxdepth,
        withdirs=withdirs,
        **kwargs,
    )


def glob(path, maxdepth=None, **kwargs):
    """
    Find files by glob-matching.

    If the path ends with '/', only folders are returned.

    We support ``"**"``,
    ``"?"`` and ``"[..]"``. We do not support ^ for pattern negation.

    The `maxdepth` option is applied on the first `**` found in the path.

    Search path names that contain embedded characters special to this
    implementation of glob may not produce expected results;
    e.g., 'foo/bar/*starredfilename*'.

    kwargs are passed to ``ls``.
    """
    mnt = get_mount(path)
    detail = kwargs.get("detail", False)

    items = mnt.glob(path, maxdepth=maxdepth, **kwargs)
    if not detail:
        return [_rewrite_path(p, mnt) for p in items]
    else:
        return {_rewrite_path(k, mnt): _rewrite_info(v, mnt) for k, v in items.items()}


def exists(path, **kwargs):
    """Is there a file at the given path?.

    Parameters
    ----------
    path: str
        path to check

    """
    return get_mount(path).exists(path, **kwargs)


def lexists(path, **kwargs):
    """Is there a file at the given path?.

    Parameters
    ----------
    path: str
        path to check

    """
    return get_mount(path).lexists(path, **kwargs)


def checksum(path):
    """Unique value for current version of file.

    If the checksum is the same from one moment to another, the contents
    are guaranteed to be the same. If the checksum changes, the contents
    *might* have changed.

    This should normally be overridden; default will probably capture
    creation/modification timestamp (which would be good) or maybe
    access timestamp (which would be bad)
    """
    return get_mount(path).checksum(path)


def size(path):
    """Size in bytes of file."""
    return get_mount(path).size(path)


def sizes(paths):
    """Size in bytes of each file in a list of paths."""
    return [size(p) for p in paths]


def isdir(path):
    """Is this entry a directory?."""
    return get_mount(path).isdir(path)


def isfile(path):
    """Is this entry a file?."""
    return get_mount(path).isfile(path)


def islink(path):
    """Is this entry a link?."""
    return get_mount(path).islink(path)


def read_text(path, encoding=None, errors=None, newline=None, **kwargs):
    """Get the contents of the file as a string.

    Parameters
    ----------
    path: str
        URL of file on this filesystems
    encoding, errors, newline: same as `open`.
    """
    return get_mount(path).read_text(path, encoding=encoding, errors=errors, newline=newline, **kwargs)


def write_text(path, data, encoding=None, errors=None, newline=None, **kwargs):
    """Write string data to file.

    Parameters
    ----------
    path: str
        URL of file on this filesystems
    data: str
        Data to write
    encoding, errors, newline: same as `open`.
    """
    return get_mount(path).write_text(
        path,
        data=data,
        encoding=encoding,
        errors=errors,
        newline=newline,
        **kwargs,
    )


def cat_file(path, start=None, end=None, **kwargs):
    """Get the content of a file.

    Parameters
    ----------
    path: URL of file on this filesystems
    start, end: int
        Bytes limits of the read. If negative, backwards from end,
        like usual python slices. Either can be None for start or
        end of file, respectively
    kwargs: passed to ``open()``.
    """
    return get_mount(path).cat_file(path, start=start, end=end, **kwargs)


def pipe_file(path, value, **kwargs):
    """Set the bytes of given file."""
    return get_mount(path).pipe_file(path, value=value, **kwargs)


def pipe(path, value=None, **kwargs):
    """Put value into path.

    (counterpart to ``cat``)

    Parameters
    ----------
    path: string or dict(str, bytes)
        If a string, a single remote location to put ``value`` bytes; if a dict,
        a mapping of {path: bytesvalue}.
    value: bytes, optional
        If using a single path, these are the bytes to put there. Ignored if
        ``path`` is a dict
    """
    return get_mount(path).pipe(path, value=value, **kwargs)


def cat_ranges(paths, starts, ends, max_gap=None, on_error="return", **kwargs):
    [
        get_mount(path).cat_ranges(path, starts, ends, max_gap=max_gap, on_error=on_error, **kwargs)
        for path in paths
    ]


def cat(path, recursive=False, on_error="raise", **kwargs):
    """Fetch (potentially multiple) paths' contents.

    Parameters
    ----------
    recursive: bool
        If True, assume the path(s) are directories, and get all the
        contained files
    on_error : "raise", "omit", "return"
        If raise, an underlying exception will be raised (converted to KeyError
        if the type is in self.missing_exceptions); if omit, keys with exception
        will simply not be included in the output; if "return", all keys are
        included in the output, but the value will be bytes or an exception
        instance.
    kwargs: passed to cat_file

    Returns
    -------
    dict of {path: contents} if there are multiple paths
    or the path has been otherwise expanded
    """
    if recursive:
        paths = find(path, maxdepth=None, **kwargs)
    else:
        paths = [path]

    return {p: cat_file(p, on_error=on_error, **kwargs) for p in paths}


def get_file(rpath, lpath, callback=NoOpCallback(), outfile=None, **kwargs):
    """Copy single remote file to local."""
    raise NotImplementedError()


def get(
    rpath,
    lpath,
    recursive=False,
    callback=NoOpCallback(),
    maxdepth=None,
    **kwargs,
):
    """Copy file(s) to local.

    Copies a specific file or tree of files (if recursive=True). If lpath
    ends with a "/", it will be assumed to be a directory, and target files
    will go within. Can submit a list of paths, which may be glob-patterns
    and will be expanded.

    Calls get_file for each source.
    """
    raise NotImplementedError()


def copy(path1, path2, recursive=False, maxdepth=None, on_error=None, **kwargs):
    """Copy between two locations.

    on_error : "raise", "ignore"
        If raise, any not-found exceptions will be raised; if ignore any
        not-found exceptions will cause the path to be skipped; defaults to
        raise unless recursive is true, where the default is ignore
    """
    mnt1 = get_mount(path1)
    mnt2 = get_mount(path2)

    path1 = mnt1.replace_mount_point(path1)
    path2 = mnt2.replace_mount_point(path2)

    if mnt1.fsid == mnt2.fsid:
        # use fs directly instead of relying on the rewrite mechanism
        return mnt1.fs.copy(
            path1,
            path2,
            recursive=recursive,
            maxdepth=maxdepth,
            on_error=on_error,
            **kwargs,
        )

    # non-local copy
    with mnt1.fs.open(path1, "rb") as f1, mnt2.fs.open(path2, "wb") as f2:
        f2.write(f1.read())


def mv(path1, path2, recursive=False, maxdepth=None, **kwargs):
    """Move between two locations.

    If the destination is on the same filesystem, this will be a true move,
    otherwise a copy and delete.

    Parameters
    ----------
    path1: str
        Source path
    path2: str
        Destination path
    recursive: bool
        If True, and path1 is a directory, move the contents; otherwise
        move the directory itself.
    maxdepth: int or None
        If recursive, limit the depth of recursion
    kwargs: passed to ``copy`` and ``rm``
    """
    mnt1 = get_mount(path1)
    mnt2 = get_mount(path2)

    if mnt1.fsid == mnt2.fsid:
        # use fs directly instead of relying on the rewrite mechanism
        return mnt1.fs.mv(
            mnt1.replace_mount_point(path1),
            mnt2.replace_mount_point(path2),
            recursive=recursive,
            maxdepth=maxdepth,
            **kwargs,
        )

    copy(path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs)
    rm(path1, recursive=recursive, maxdepth=maxdepth)


def rm(path, recursive=False, maxdepth=None):
    """Delete files.

    Parameters
    ----------
    path: str or list of str
        File(s) to delete.
    recursive: bool
        If file(s) are directories, recursively delete contents and then
        also remove the directory
    maxdepth: int or None
        Depth to pass to walk for finding files to delete, if recursive.
        If None, there will be no limit and infinite recursion may be
        possible.
    """
    mnt = get_mount(path)

    if recursive:
        paths = find(path, maxdepth=maxdepth)
    else:
        paths = [path]

    for path in paths:
        mnt.rm(path)


def open(
    path,
    mode="rb",
    block_size=None,
    cache_options=None,
    compression=None,
    **kwargs,
):
    """
    Return a file-like object from the filesystem.

    The resultant instance must function correctly in a context ``with``
    block.

    Parameters
    ----------
    path: str
        Target file
    mode: str like 'rb', 'w'
        See builtin ``open()``
    block_size: int
        Some indication of buffering - this is a value in bytes
    cache_options : dict, optional
        Extra arguments to pass through to the cache.
    compression: string or None
        If given, open file using compression codec. Can either be a compression
        name (a key in ``fsspec.compression.compr``) or "infer" to guess the
        compression from the filename suffix.
    encoding, errors, newline: passed on to TextIOWrapper for text mode
    """
    return get_mount(path).open(
        path,
        mode=mode,
        block_size=block_size,
        cache_options=cache_options,
        compression=compression,
        **kwargs,
    )


def touch(path, truncate=True, **kwargs):
    """Create empty file, or update timestamp.

    Parameters
    ----------
    path: str
        file location
    truncate: bool
        If True, always set file size to 0; if False, update timestamp and
        leave file unchanged, if backend allows this
    """
    return get_mount(path).touch(path, truncate=truncate, **kwargs)


def ukey(path):
    """Hash of file properties, to tell if it has changed."""
    return get_mount(path).ukey(path)


def read_block(path, offset, length, delimiter=None):
    r"""Read a block of bytes from.

    Starting at ``offset`` of the file, read ``length`` bytes.  If
    ``delimiter`` is set then we ensure that the read starts and stops at
    delimiter boundaries that follow the locations ``offset`` and ``offset
    + length``.  If ``offset`` is zero then we start at zero.  The
    bytestring returned WILL include the end delimiter string.

    If offset+length is beyond the eof, reads to eof.

    Parameters
    ----------
    path: string
        Path to filename
    offset: int
        Byte offset to start read
    length: int
        Number of bytes to read. If None, read to end.
    delimiter: bytes (optional)
        Ensure reading starts and stops at delimiter bytestring

    Examples
    --------
    >>> fs.read_block('data/file.csv', 0, 13)
    b'Alice, 100\\nBo'
    >>> fs.read_block('data/file.csv', 0, 13, delimiter=b'\\n')
    b'Alice, 100\\nBob, 200\\n'

    Use ``length=None`` to read to the end of the file.
    >>> fs.read_block('data/file.csv', 0, None, delimiter=b'\\n')
    b'Alice, 100\\nBob, 200\\nCharlie, 300'

    See Also
    --------
    :func:`fsspec.utils.read_block`
    """
    return get_mount(path).read_block(path, offset, length, delimiter=delimiter)


def created(path):
    """Creation time of file."""
    return get_mount(path).created(path)


def modified(path):
    """Return the modified timestamp of a file as a datetime.datetime."""
    return get_mount(path).modified(path)


# ------------------------------------------------------------------------
# Aliases


def read_bytes(path, start=None, end=None, **kwargs):
    """Alias of `cat_file`."""
    return cat_file(path, start=start, end=end, **kwargs)


def write_bytes(path, value, **kwargs):
    """Alias of `pipe_file`."""
    pipe_file(path, value, **kwargs)


def makedir(path, create_parents=True, **kwargs):
    """Alias of `mkdir`."""
    return mkdir(path, create_parents=create_parents, **kwargs)


def mkdirs(path, exist_ok=False):
    """Alias of `makedirs`."""
    return makedirs(path, exist_ok=exist_ok)


def listdir(self, path, detail=True, **kwargs):
    """Alias of `ls`."""
    return ls(path, detail=detail, **kwargs)


def cp(path1, path2, **kwargs):
    """Alias of `copy`."""
    return copy(path1, path2, **kwargs)


def move(path1, path2, **kwargs):
    """Alias of `mv`."""
    return mv(path1, path2, **kwargs)


def stat(self, path, **kwargs):
    """Alias of `info`."""
    return self.info(path, **kwargs)


def disk_usage(path, total=True, maxdepth=None, **kwargs):
    """Alias of `du`."""
    return du(path, total=total, maxdepth=maxdepth, **kwargs)


def rename(path1, path2, **kwargs):
    """Alias of `mv`."""
    return mv(path1, path2, **kwargs)


def delete(path, recursive=False, maxdepth=None):
    """Alias of `rm`."""
    return rm(path, recursive=recursive, maxdepth=maxdepth)


def upload(lpath, rpath, recursive=False, **kwargs):
    """Alias of `put`."""
    raise NotImplementedError()


def download(rpath, lpath, recursive=False, **kwargs):
    """Alias of `get`."""
    raise NotImplementedError()


def sign(path, expiration=100, **kwargs):
    """Create a signed URL representing the given path.

    Some implementations allow temporary URLs to be generated, as a
    way of delegating credentials.

    Parameters
    ----------
    path : str
         The path on the filesystem
    expiration : int
        Number of seconds to enable the URL for (if supported)

    Returns
    -------
    URL : str
        The signed URL

    Raises
    ------
    NotImplementedError : if method is not implemented for a filesystem
    """
    return get_mount(path).sign(path, expiration=expiration, **kwargs)
