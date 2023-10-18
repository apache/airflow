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
from __future__ import annotations

import uuid
from unittest import mock

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import stringify_path
from s3fs import S3FileSystem

from airflow.io.store import _STORE_CACHE, attach
from airflow.io.store.path import ObjectStoragePath

FAKE = "file:///fake"
MNT = "file:///mnt/warehouse"
FOO = "file:///mnt/warehouse/foo"
BAR = FOO


class FakeRemoteFileSystem(LocalFileSystem):
    @property
    def fsid(self):
        return "fakefs"

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = stringify_path(path)
        i = path.find("://")
        return path[i + 3 :] if i > 0 else path


class TestFs:
    def test_alias(self):
        store = attach("s3")
        assert isinstance(store.fs, S3FileSystem)

        store = attach("file", alias="local")
        assert isinstance(store.fs, LocalFileSystem)
        assert "local" in _STORE_CACHE

    def test_init_objectstoragepath(self):
        path = ObjectStoragePath("s3://bucket/key/part1/part2")
        assert path.bucket == "bucket"
        assert path.key == "key/part1/part2"
        assert path._protocol == "s3"

    def test_read_write(self):
        o = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")

        with o.open("wb") as f:
            f.write(b"foo")

        assert o.open("rb").read() == b"foo"

        o.unlink()

    def test_ls(self):
        dirname = str(uuid.uuid4())
        filename = str(uuid.uuid4())

        d = ObjectStoragePath(f"file:///tmp/{dirname}")
        d.mkdir(create_parents=True)
        o = d / filename
        o.touch()

        data = d.ls()
        assert len(data) == 1
        assert data[0]["name"] == o

        data = d.ls(detail=False)
        assert data == [o]

        d.unlink(recursive=True)

        assert not o.exists()

    def test_find(self):
        dirname = str(uuid.uuid4())
        filename = str(uuid.uuid4())

        d = ObjectStoragePath(f"file:///tmp/{dirname}")
        d.mkdir(create_parents=True)
        o = d / filename
        o.touch()

        data = d.find("")
        assert len(data) == 1
        assert data == [o]

        data = d.ls(detail=True)
        assert len(data) == 1
        assert data[0]["name"] == o

        d.unlink(recursive=True)

    @pytest.mark.parametrize(
        "fn, args, fn2, path, expected_args, expected_kwargs",
        [
            ("du", {}, "du", FOO, BAR, {"total": True, "maxdepth": None, "withdirs": False}),
            ("exists", {}, "exists", FOO, ObjectStoragePath(BAR), {}),
            ("checksum", {}, "checksum", FOO, ObjectStoragePath(BAR), {}),
            ("size", {}, "size", FOO, ObjectStoragePath(BAR), {}),
            ("is_dir", {}, "isdir", FOO, ObjectStoragePath(BAR), {}),
            ("is_file", {}, "isfile", FOO, ObjectStoragePath(BAR), {}),
            # ("is_symlink", {}, "islink", FOO, ObjectStoragePath(BAR), {}),
            ("touch", {}, "touch", FOO, BAR, {"truncate": True}),
            ("mkdir", {}, "mkdir", FOO, BAR, {"create_parents": True}),
            ("read_text", {}, "read_text", FOO, BAR, {"encoding": None, "errors": None, "newline": None}),
            ("read_bytes", {}, "read_bytes", FOO, BAR, {"start": None, "end": None}),
            ("rm", {}, "rm", FOO, BAR, {"maxdepth": None, "recursive": False}),
            ("rmdir", {}, "rmdir", FOO, BAR, {}),
            ("write_bytes", {"data": b"foo"}, "pipe_file", FOO, ObjectStoragePath(BAR), {"value": b"foo"}),
            (
                "write_text",
                {"data": "foo"},
                "write_text",
                FOO,
                BAR,
                {"value": "foo", "encoding": None, "errors": None, "newline": None},
            ),
            ("ukey", {}, "ukey", FOO, BAR, {}),
        ],
    )
    def test_standard_api(self, fn, args, fn2, path, expected_args, expected_kwargs):
        _fs = mock.Mock()
        _fs._strip_protocol.return_value = "/"
        _fs.conn_id = "fake"

        store = attach(protocol="fakefs", fs_type=_fs)
        o = ObjectStoragePath(path, store=store)

        getattr(o, fn)(**args)
        getattr(store.fs, fn2).assert_called_once_with(expected_args, **expected_kwargs)

    def test_move_local(self):
        _from = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")
        _to = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")

        _from.touch()
        _from.move(_to)
        assert _to.exists()
        assert not _from.exists()

        _to.unlink()

    def test_move_remote(self):
        attach("fakefs", fs_type=FakeRemoteFileSystem())

        _from = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")
        print(_from)
        _to = ObjectStoragePath(f"fakefs:///tmp/{str(uuid.uuid4())}")
        print(_to)

        _from.touch()
        _from.move(_to)
        assert not _from.exists()
        assert _to.exists()

        _to.unlink()
