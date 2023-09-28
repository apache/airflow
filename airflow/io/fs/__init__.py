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

import os.path
from urllib.parse import urlparse

from fsspec import AbstractFileSystem

from airflow.io.fsspec import SCHEME_TO_FS

MOUNT_POINTS: dict[str:AbstractFileSystem] = {}


def _get_mount_point(path: str) -> str:
    """
    Get the mount point for a given path.

    :param path: the path to get the mount point for
    :type path: str
    :return: the mount point
    :rtype: str
    """
    mount_point = None
    mount_points = sorted(MOUNT_POINTS.keys(), key=len, reverse=True)

    for prefix in mount_points:
        if os.path.commonprefix([prefix, path]) == prefix:
            mount_point = prefix

    if mount_point is None:
        raise ValueError(f"No mount point found for path: {path}")

    return mount_point


def _replace_mount_point(path: str) -> str:
    """
    Replace the mount point in a path with another mount point.

    :param path: the path to replace the mount point in
    :type path: str
    :param mount_point: the mount point to replace the mount point with
    :type mount_point: str
    :return: the path with the mount point replaced
    :rtype: str
    """
    return path.replace(_get_mount_point(path), "")


def mount(
    source: str, mount_point: str, conn_id: str | None = None, encryption_type: str | None = ""
) -> None:
    """
    Mount a filesystem or object storage to a mount point.

    :param source: the source path to mount
    :type source: str
    :param mount_point: the target mount point
    :type mount_point: str
    :param conn_id: the connection to use to connect to the filesystem
    :type conn_id: str
    :param encryption_type: the encryption type to use to connect to the filesystem
    :type encryption_type: str
    """
    if mount_point in MOUNT_POINTS:
        raise ValueError(f"Mount point {mount_point} already mounted")

    if conn_id is None:
        conn_id = "aws_default"

    if encryption_type is None:
        encryption_type = "AES256"

    uri = urlparse(mount_point)
    scheme = uri.scheme

    if scheme not in SCHEME_TO_FS:
        raise ValueError(f"No registered filesystem for scheme: {scheme}")

    fs = SCHEME_TO_FS[scheme](dict())
    MOUNT_POINTS[source] = fs


def unmount(mount_point: str) -> None:
    """
    Unmount a filesystem or object storage from a mount point.

    :param mount_point: the mount point to unmount
    :type mount_point: str
    """
    if mount_point not in MOUNT_POINTS:
        raise ValueError(f"Mount point {mount_point} not mounted")

    del MOUNT_POINTS[mount_point]


def get_fs(mount_point: str) -> AbstractFileSystem:
    """
    Get the filesystem for a given mount point or path.

    :param mount_point: the path to get the filesystem for
    :type mount_point: str
    :return: the filesystem
    :rtype: AbstractFileSystem
    """
    return MOUNT_POINTS[_get_mount_point(mount_point)]


def mkdir(path, create_parents=True, **kwargs):
    """
    Create directory entry at path

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
    get_fs(path).mkdir(_replace_mount_point(path), create_parents=create_parents, **kwargs)


def makedirs(path, exist_ok=False):
    """Recursively make directories

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
    get_fs(path).makedirs(_replace_mount_point(path), exist_ok=exist_ok)


def rmdir(path):
    """Remove a directory, if empty"""
    get_fs(path).rmdir(_replace_mount_point(path))


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
    return get_fs(path).ls(_replace_mount_point(path), detail=detail, **kwargs)


def walk(path, maxdepth=None, topdown=True, on_error="omit", **kwargs):
    """Return all files belows path

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
    return get_fs(path).walk(
        _replace_mount_point(path),
        maxdepth=maxdepth,
        topdown=topdown,
        on_error=on_error,
        **kwargs,
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
    return get_fs(path).find(
        _replace_mount_point(path),
        maxdepth=maxdepth,
        withdirs=withdirs,
        detail=detail,
        **kwargs,
    )


def du(path, total=True, maxdepth=None, withdirs=False, **kwargs):
    """Space used by files and optionally directories within a path

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
    return get_fs(path).du(
        _replace_mount_point(path),
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
    return get_fs(path).glob(_replace_mount_point(path), maxdepth=maxdepth, **kwargs)


def exists(path, **kwargs):
    """Is there a file at the given path?

    Parameters
    ----------
    path: str
        path to check

    """
    return get_fs(path).exists(_replace_mount_point(path), **kwargs)

