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

from airflow.io.store.path import ObjectStoragePath


def cp(
    path1: str | ObjectStoragePath,
    path2: str | ObjectStoragePath,
    recursive: bool = False,
    maxdepth: int = None,
    on_error: str = None,
    **kwargs,
):
    """Copy between two locations.
    on_error : "raise", "ignore"
        If raise, any not-found exceptions will be raised; if ignore any
        not-found exceptions will cause the path to be skipped; defaults to
        raise unless recursive is true, where the default is ignore.
    """
    if isinstance(path1, str):
        path1 = ObjectStoragePath(path1)

    if isinstance(path2, str):
        path2 = ObjectStoragePath(path2)

    if path1.samefile(path2):
        path1._store.fs.copy(
            str(path1),
            str(path2),
            recursive=recursive,
            maxdepth=maxdepth,
            on_error=on_error,
            **kwargs,
        )
    else:
        # non-local copy
        with path1.open("rb") as f1, path2.open("wb") as f2:
            f2.write(f1.read())


def mv(
    path1: str | ObjectStoragePath,
    path2: str | ObjectStoragePath,
    recursive=False,
    maxdepth=None,
    **kwargs,
):
    """Move between two locations."""
    if isinstance(path1, str):
        path1 = ObjectStoragePath(path1)

    if isinstance(path2, str):
        path2 = ObjectStoragePath(path2)

    if path1.samestore(path2):
        path1._store.fs.move(str(path1), str(path2), recursive=recursive, maxdepth=maxdepth, **kwargs)
    else:
        cp(path1, path2, recursive=recursive, maxdepth=maxdepth, **kwargs)
        path1.unlink(recursive=recursive, maxdepth=maxdepth)


copy = cp
move = mv
