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
from stat import S_ISDIR, S_ISREG
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import stringify_path

from airflow.io import _register_filesystems, get_fs
from airflow.io.path import ObjectStoragePath
from airflow.io.store import _STORE_CACHE, ObjectStore, attach
from airflow.utils.module_loading import qualname

FAKE = "file:///fake"
MNT = "file:///mnt/warehouse"
FOO = "file:///mnt/warehouse/foo"
BAR = FOO


class FakeRemoteFileSystem(LocalFileSystem):
    id = "fakefs"
    auto_mk_dir = True

    @property
    def fsid(self):
        return self.id

    @classmethod
    def _strip_protocol(cls, path) -> str:
        path = stringify_path(path)
        i = path.find("://")
        return path[i + 3 :] if i > 0 else path


def get_fs_no_storage_options(_: str):
    return LocalFileSystem()


class TestFs:
    def setup_class(self):
        self._store_cache = _STORE_CACHE.copy()

    def teardown(self):
        _STORE_CACHE.clear()
        _STORE_CACHE.update(self._store_cache)

    def test_alias(self):
        store = attach("file", alias="local")
        assert isinstance(store.fs, LocalFileSystem)
        assert "local" in _STORE_CACHE

    def test_init_objectstoragepath(self):
        path = ObjectStoragePath("file://bucket/key/part1/part2")
        assert path.bucket == "bucket"
        assert path.key == "key/part1/part2"
        assert path.protocol == "file"
        assert path.path == "bucket/key/part1/part2"

        path2 = ObjectStoragePath(path / "part3")
        assert path2.bucket == "bucket"
        assert path2.key == "key/part1/part2/part3"
        assert path2.protocol == "file"
        assert path2.path == "bucket/key/part1/part2/part3"

        path3 = ObjectStoragePath(path2 / "2023")
        assert path3.bucket == "bucket"
        assert path3.key == "key/part1/part2/part3/2023"
        assert path3.protocol == "file"
        assert path3.path == "bucket/key/part1/part2/part3/2023"

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
        d.mkdir(parents=True)
        o = d / filename
        o.touch()

        data = list(d.iterdir())
        assert len(data) == 1
        assert data[0] == o

        d.rmdir(recursive=True)

        assert not o.exists()

    @pytest.fixture()
    def fake_fs(self):
        fs = mock.Mock()
        fs._strip_protocol.return_value = "/"
        fs.conn_id = "fake"
        return fs

    def test_objectstoragepath_init_conn_id_in_uri(self, fake_fs):
        fake_fs.stat.return_value = {"stat": "result"}
        attach(protocol="fake", conn_id="fake", fs=fake_fs)
        p = ObjectStoragePath("fake://fake@bucket/path")
        assert p.stat() == {"stat": "result", "conn_id": "fake", "protocol": "fake"}

    @pytest.mark.parametrize(
        "fn, args, fn2, path, expected_args, expected_kwargs",
        [
            ("checksum", {}, "checksum", FOO, FakeRemoteFileSystem._strip_protocol(BAR), {}),
            ("size", {}, "size", FOO, FakeRemoteFileSystem._strip_protocol(BAR), {}),
            (
                "sign",
                {"expiration": 200, "extra": "xtra"},
                "sign",
                FOO,
                FakeRemoteFileSystem._strip_protocol(BAR),
                {"expiration": 200, "extra": "xtra"},
            ),
            ("ukey", {}, "ukey", FOO, FakeRemoteFileSystem._strip_protocol(BAR), {}),
            (
                "read_block",
                {"offset": 0, "length": 1},
                "read_block",
                FOO,
                FakeRemoteFileSystem._strip_protocol(BAR),
                {"delimiter": None, "length": 1, "offset": 0},
            ),
        ],
    )
    def test_standard_extended_api(self, fake_fs, fn, args, fn2, path, expected_args, expected_kwargs):
        store = attach(protocol="file", conn_id="fake", fs=fake_fs)
        o = ObjectStoragePath(path, conn_id="fake")

        getattr(o, fn)(**args)
        getattr(store.fs, fn2).assert_called_once_with(expected_args, **expected_kwargs)

    def test_stat(self):
        with NamedTemporaryFile() as f:
            o = ObjectStoragePath(f"file://{f.name}")
            assert o.stat().st_size == 0
            assert S_ISREG(o.stat().st_mode)
            assert S_ISDIR(o.parent.stat().st_mode)

    def test_bucket_key_protocol(self):
        bucket = "bkt"
        key = "yek"
        protocol = "s3"

        o = ObjectStoragePath(f"{protocol}://{bucket}/{key}")
        assert o.bucket == bucket
        assert o.container == bucket
        assert o.key == f"{key}"
        assert o.protocol == protocol

    def test_cwd_home(self):
        assert ObjectStoragePath.cwd()
        assert ObjectStoragePath.home()

    def test_replace(self):
        o = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")
        i = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")

        o.touch()
        i.touch()

        assert i.size() == 0

        txt = "foo"
        o.write_text(txt)
        e = o.replace(i)
        assert o.exists() is False
        assert i == e
        assert e.size() == len(txt)

        e.unlink()

    def test_move_local(self):
        _from = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")
        _to = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")

        _from.touch()
        _from.move(_to)
        assert _to.exists()
        assert not _from.exists()

        _to.unlink()

    def test_move_remote(self):
        attach("fakefs", fs=FakeRemoteFileSystem())

        _from = ObjectStoragePath(f"file:///tmp/{str(uuid.uuid4())}")
        print(_from)
        _to = ObjectStoragePath(f"fakefs:///tmp/{str(uuid.uuid4())}")
        print(_to)

        _from.touch()
        _from.move(_to)
        assert not _from.exists()
        assert _to.exists()

        _to.unlink()

    def test_copy_remote_remote(self):
        # foo = xxx added to prevent same fs token
        attach("ffs", fs=FakeRemoteFileSystem(auto_mkdir=True, foo="bar"))
        attach("ffs2", fs=FakeRemoteFileSystem(auto_mkdir=True, foo="baz"))

        dir_src = f"/tmp/{str(uuid.uuid4())}"
        dir_dst = f"/tmp/{str(uuid.uuid4())}"
        key = "foo/bar/baz.txt"

        # note we are dealing with object storage characteristics
        # while working on a local filesystem, so it might feel not intuitive
        _from = ObjectStoragePath(f"ffs://{dir_src}")
        _from_file = _from / key
        _from_file.touch()
        assert _from_file.exists()

        _to = ObjectStoragePath(f"ffs2://{dir_dst}")
        _from.copy(_to)

        assert _to.exists()
        assert _to.is_dir()
        assert (_to / _from.key / key).exists()
        assert (_to / _from.key / key).is_file()

        _from.rmdir(recursive=True)
        _to.rmdir(recursive=True)

    def test_serde_objectstoragepath(self):
        path = "file://bucket/key/part1/part2"
        o = ObjectStoragePath(path)

        s = o.serialize()
        assert s["path"] == path
        d = ObjectStoragePath.deserialize(s, 1)
        assert o == d

        o = ObjectStoragePath(path, my_setting="foo")
        s = o.serialize()
        assert "my_setting" in s["kwargs"]
        d = ObjectStoragePath.deserialize(s, 1)
        assert o == d

        store = attach("filex", conn_id="mock")
        o = ObjectStoragePath(path, store=store)
        s = o.serialize()
        assert s["kwargs"]["store"] == store

        d = ObjectStoragePath.deserialize(s, 1)
        assert o == d

    def test_serde_store(self):
        store = attach("file", conn_id="mock")
        s = store.serialize()
        d = ObjectStore.deserialize(s, 1)

        assert s["protocol"] == "file"
        assert s["conn_id"] == "mock"
        assert s["filesystem"] == qualname(LocalFileSystem)
        assert store == d

        store = attach("localfs", fs=LocalFileSystem())
        s = store.serialize()
        d = ObjectStore.deserialize(s, 1)

        assert s["protocol"] == "localfs"
        assert s["conn_id"] is None
        assert s["filesystem"] == qualname(LocalFileSystem)
        assert store == d

    def test_backwards_compat(self):
        _register_filesystems.cache_clear()
        from airflow.io import _BUILTIN_SCHEME_TO_FS as SCHEMES

        try:
            SCHEMES["file"] = get_fs_no_storage_options  # type: ignore[call-arg]

            assert get_fs("file")

            with pytest.raises(AttributeError):
                get_fs("file", storage_options={"foo": "bar"})

        finally:
            # Reset the cache to avoid side effects
            _register_filesystems.cache_clear()
