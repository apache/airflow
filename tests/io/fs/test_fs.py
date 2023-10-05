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

import os.path
import uuid
from unittest import mock

import pytest
from fsspec.implementations.local import LocalFileSystem
from s3fs import S3FileSystem

from airflow.io import fs

FAKE = "/mnt/fake"
MNT = "/mnt/warehouse"
FOO = "/mnt/warehouse/foo"
BAR = "/foo"


class FakeRemoteFileSystem(LocalFileSystem):
    @property
    def fsid(self):
        return "fakefs"


class TestFs:
    def test_mount(self):
        fs.mount("s3://warehouse/", MNT)

        assert isinstance(fs.get_fs("/mnt/warehouse"), S3FileSystem)
        assert fs.get_mount(MNT).replace_mount_point(FOO) == "warehouse/foo"


        fs.unmount(MNT)

    def test_mount_without_mountpoint(self):
        mnt = fs.mount("s3://warehouse/")

        assert isinstance(mnt.fs, S3FileSystem)
        assert mnt.replace_mount_point("/foo") == "warehouse/foo"
        assert mnt / "foo" == os.path.join(mnt.mount_point, "foo")

        fs.unmount(mnt)

    def test_unmount(self):
        fs.mount("s3://warehouse/", MNT)
        fs.unmount(MNT)

        with pytest.raises(ValueError):
            fs.get_fs("/mnt/warehouse")

    def test_read_write(self):
        fs.mount("file:///tmp/", MNT)

        filename = str(uuid.uuid4())
        output_file = os.path.join(MNT, filename)

        with fs.open(output_file, "wb") as f:
            f.write(b"foo")

        input_file = os.path.join(MNT, filename)
        assert fs.open(input_file, "rb").read() == b"foo"

        fs.rm(input_file)
        fs.unmount(MNT)

    def test_ls(self):
        fs.mount("file:///tmp/", MNT)
        dirname = os.path.join(MNT, str(uuid.uuid4()))
        filename = os.path.join(dirname, str(uuid.uuid4()))

        fs.makedirs(dirname)
        fs.touch(filename)

        data = fs.ls(dirname)
        assert len(data) == 1
        assert data[0]["name"] == filename
        assert "original_name" in data[0]

        data = fs.ls(dirname, detail=False)
        assert data == [filename]

        fs.rm(dirname, recursive=True)
        fs.unmount(MNT)

    def test_find(self):
        fs.mount("file:///tmp/", MNT)
        dirname = os.path.join(MNT, str(uuid.uuid4()))
        filename = os.path.join(dirname, str(uuid.uuid4()))

        fs.makedirs(dirname)
        fs.touch(filename)

        data = fs.find(dirname)
        assert len(data) == 1
        assert data == [filename]

        data = fs.ls(dirname, detail=True)
        assert len(data) == 1
        assert data[0]["name"] == filename
        assert "original_name" in data[0]

        fs.rm(dirname, recursive=True)
        fs.unmount(MNT)

    @pytest.mark.parametrize(
        "fn, args, fn2, path, expected_args, expected_kwargs",
    [
        ("du", {}, "du", FOO, BAR, {"total": True, "maxdepth": None, "withdirs": False}),
        ("created", {}, "created", FOO, BAR, {}),
        ("exists", {}, "exists", FOO, BAR, {}),
        ("lexists", {}, "lexists", FOO, BAR, {}),
        ("checksum", {}, "checksum", FOO, BAR, {}),
        ("size", {}, "size", FOO, BAR, {}),
        ("isdir", {}, "isdir", FOO, BAR, {}),
        ("isfile", {}, "isfile", FOO, BAR, {}),
        ("islink", {}, "islink", FOO, BAR, {}),
        ("makedirs", {}, "makedirs", FOO, BAR, {"exist_ok": False}),
        ("touch", {}, "touch", FOO, BAR, {"truncate": True}),
        ("mkdir", {}, "mkdir", FOO, BAR, {"create_parents": True}),
        ("modified", {}, "modified", FOO, BAR, {}),
        ("read_text", {}, "read_text", FOO, BAR, {"encoding": None, "errors": None, "newline": None}),
        ("read_bytes", {}, "cat_file", FOO, BAR, {"start": None, "end": None}),
        ("rm", {}, "rm", FOO, BAR, {}),
        ("rmdir", {}, "rmdir", FOO, BAR, {}),
        ("cat_file", {}, "cat_file", FOO, BAR, {"end": None, "start": None}),
        ("pipe", {}, "pipe", FOO, BAR, {"value": None}),
        ("pipe_file", {"value": b"foo"}, "pipe_file", FOO, BAR, {"value": b"foo"}),
        ("write_bytes", {"value": b"foo"}, "pipe_file", FOO, BAR, {"value": b"foo"}),
        ("write_text", {"data": "foo"}, "write_text", FOO, BAR, {"data": "foo", "encoding": None, "errors": None, "newline": None}),
        ("ukey", {}, "ukey", FOO, BAR, {}),
    ])
    def test_standard_api(self, fn, args, fn2, path, expected_args, expected_kwargs):
        _fs = mock.Mock()
        _fs._strip_protocol.return_value = "/"

        fs.mount(source="fakefs:///", mount_point=MNT, fs_type=_fs)

        getattr(fs, fn)(path, **args)
        getattr(_fs, fn2).assert_called_once_with(expected_args, **expected_kwargs)

        fs.unmount(MNT)

    def test_copy(self):
        pass

    def test_move_local(self):
        fs.mount("file:///tmp/", MNT)
        _from = os.path.join(MNT, str(uuid.uuid4()))
        _to = os.path.join(MNT, str(uuid.uuid4()))

        fs.touch(_from)
        fs.move(_from, _to)
        assert fs.exists(_to)
        assert not fs.exists(_from)

        fs.rm(_to)
        fs.unmount(MNT)

    def test_move_remote(self):
        fs.mount("file:///tmp/", MNT)
        fs.mount("file:///tmp/", FAKE, fs_type=FakeRemoteFileSystem())

        _from = os.path.join(MNT, str(uuid.uuid4()))
        _to = os.path.join(FAKE, str(uuid.uuid4()))

        fs.touch(_from)
        fs.move(_from, _to)
        assert fs.exists(_to)
        assert not fs.exists(_from)

        fs.rm(_to)
        fs.unmount(MNT)
        fs.unmount(FAKE)

