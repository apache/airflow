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
from typing import Any, ClassVar
from unittest import mock

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem

from airflow.sdk import Asset, ObjectStoragePath
from airflow.sdk.io import attach
from airflow.sdk.io.store import _STORE_CACHE, ObjectStore
from airflow.sdk.module_loading import qualname


def test_init():
    path = ObjectStoragePath("s3://bucket/key/part1/part2")
    assert path.bucket == "bucket"
    assert path.key == "key/part1/part2"
    assert path.protocol == "s3"
    assert path.path == "bucket/key/part1/part2"

    path2 = ObjectStoragePath(path / "part3")
    assert path2.bucket == "bucket"
    assert path2.key == "key/part1/part2/part3"
    assert path2.protocol == "s3"
    assert path2.path == "bucket/key/part1/part2/part3"

    path3 = ObjectStoragePath(path2 / "2023")
    assert path3.bucket == "bucket"
    assert path3.key == "key/part1/part2/part3/2023"
    assert path3.protocol == "s3"
    assert path3.path == "bucket/key/part1/part2/part3/2023"


@pytest.mark.parametrize("input_str", ("file:///tmp/foo", "s3://conn_id@bucket/test.txt"))
def test_str(input_str):
    o = ObjectStoragePath(input_str)
    assert str(o) == input_str


def test_cwd():
    assert ObjectStoragePath.cwd()


def test_home():
    assert ObjectStoragePath.home()


def test_lazy_load():
    o = ObjectStoragePath("file:///tmp/foo")
    with pytest.raises(AttributeError):
        assert o._fs_cached

    assert o.fs is not None
    assert o._fs_cached
    # Clear the cache to avoid side effects in other tests below
    _STORE_CACHE.clear()


class _FakeRemoteFileSystem(MemoryFileSystem):
    protocol = ("s3", "fakefs", "ffs", "ffs2")
    root_marker = ""
    store: ClassVar[dict[str, Any]] = {}
    pseudo_dirs = [""]

    def __init__(self, *args, **kwargs):
        self.conn_id = kwargs.pop("conn_id", None)
        super().__init__(*args, **kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        for protocol in cls.protocol:
            if path.startswith(f"{protocol}://"):
                return path[len(f"{protocol}://") :]
        if "::" in path or "://" in path:
            return path.rstrip("/")
        path = path.lstrip("/").rstrip("/")
        return path


class TestAttach:
    FAKE = "ffs:///fake"
    MNT = "ffs:///mnt/warehouse"
    FOO = "ffs:///mnt/warehouse/foo"
    BAR = FOO

    @pytest.fixture(autouse=True)
    def restore_cache(self):
        cache = _STORE_CACHE.copy()
        yield
        _STORE_CACHE.clear()
        _STORE_CACHE.update(cache)

    @pytest.fixture
    def fake_files(self):
        obj = _FakeRemoteFileSystem()
        obj.touch(self.FOO)
        try:
            yield
        finally:
            _FakeRemoteFileSystem.store.clear()
            _FakeRemoteFileSystem.pseudo_dirs[:] = [""]

    def test_alias(self):
        store = attach("file", alias="local")
        assert isinstance(store.fs, LocalFileSystem)
        assert {"local": store} == _STORE_CACHE

    def test_objectstoragepath_init_conn_id_in_uri(self):
        attach(protocol="fake", conn_id="fake", fs=_FakeRemoteFileSystem(conn_id="fake"))
        p = ObjectStoragePath("fake://fake@bucket/path")
        p.touch()
        fsspec_info = p.fs.info(p.path)
        assert p.stat() == {**fsspec_info, "conn_id": "fake", "protocol": "fake"}

    @pytest.mark.parametrize(
        ("fn", "args", "fn2", "path", "expected_args", "expected_kwargs"),
        [
            ("checksum", {}, "checksum", FOO, _FakeRemoteFileSystem._strip_protocol(BAR), {}),
            ("size", {}, "size", FOO, _FakeRemoteFileSystem._strip_protocol(BAR), {}),
            (
                "sign",
                {"expiration": 200, "extra": "xtra"},
                "sign",
                FOO,
                _FakeRemoteFileSystem._strip_protocol(BAR),
                {"expiration": 200, "extra": "xtra"},
            ),
            ("ukey", {}, "ukey", FOO, _FakeRemoteFileSystem._strip_protocol(BAR), {}),
            (
                "read_block",
                {"offset": 0, "length": 1},
                "read_block",
                FOO,
                _FakeRemoteFileSystem._strip_protocol(BAR),
                {"delimiter": None, "length": 1, "offset": 0},
            ),
        ],
    )
    def test_standard_extended_api(self, fake_files, fn, args, fn2, path, expected_args, expected_kwargs):
        fs = _FakeRemoteFileSystem()
        attach(protocol="ffs", conn_id="fake", fs=fs)
        with mock.patch.object(fs, fn2) as method:
            o = ObjectStoragePath(path, conn_id="fake")
            getattr(o, fn)(**args)
            method.assert_called_once_with(expected_args, **expected_kwargs)


class TestRemotePath:
    @pytest.fixture(autouse=True)
    def fake_fs(self, monkeypatch):
        monkeypatch.setattr(ObjectStoragePath, "_fs_factory", lambda *a, **k: _FakeRemoteFileSystem())

    def test_bucket_key_protocol(self):
        bucket = "bkt"
        key = "yek"
        protocol = "s3"

        o = ObjectStoragePath(f"{protocol}://{bucket}/{key}")
        assert o.bucket == bucket
        assert o.container == bucket
        assert o.key == key
        assert o.protocol == protocol


class TestLocalPath:
    @pytest.fixture
    def target(self, tmp_path):
        tmp = tmp_path.joinpath(str(uuid.uuid4()))
        tmp.touch()
        return tmp.as_posix()

    @pytest.fixture
    def another(self, tmp_path):
        tmp = tmp_path.joinpath(str(uuid.uuid4()))
        tmp.touch()
        return tmp.as_posix()

    def test_ls(self, tmp_path, target):
        d = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        o = ObjectStoragePath(f"file://{target}")

        data = list(d.iterdir())
        assert len(data) == 1
        assert data[0] == o

        d.rmdir(recursive=True)
        assert not o.exists()

    def test_read_write(self, target):
        o = ObjectStoragePath(f"file://{target}")
        with o.open("wb") as f:
            f.write(b"foo")
        assert o.open("rb").read() == b"foo"
        o.unlink()

    def test_read_line_by_line(self, target):
        o = ObjectStoragePath(f"file://{target}")
        with o.open("wb") as f:
            f.write(b"foo\nbar\n")
        with o.open("rb") as f:
            lines = list(f)
        assert lines == [b"foo\n", b"bar\n"]
        o.unlink()

    def test_stat(self, target):
        o = ObjectStoragePath(f"file://{target}")
        assert o.stat().st_size == 0
        assert S_ISREG(o.stat().st_mode)
        assert S_ISDIR(o.parent.stat().st_mode)

    def test_replace(self, target, another):
        o = ObjectStoragePath(f"file://{target}")
        i = ObjectStoragePath(f"file://{another}")
        assert i.size() == 0

        txt = "foo"
        o.write_text(txt)
        e = o.replace(i)
        assert o.exists() is False
        assert i == e
        assert e.size() == len(txt)

    def test_hash(self, target, another):
        file_uri_1 = f"file://{target}"
        file_uri_2 = f"file://{another}"
        s = set()
        for _ in range(10):
            s.add(ObjectStoragePath(file_uri_1))
            s.add(ObjectStoragePath(file_uri_2))
        assert len(s) == 2

    def test_is_relative_to(self, tmp_path, target):
        o1 = ObjectStoragePath(f"file://{target}")
        o2 = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        o3 = ObjectStoragePath(f"file:///{uuid.uuid4()}")
        assert o1.is_relative_to(o2)
        assert not o1.is_relative_to(o3)

    def test_relative_to(self, tmp_path, target):
        o1 = ObjectStoragePath(f"file://{target}")
        o2 = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        o3 = ObjectStoragePath(f"file:///{uuid.uuid4()}")
        assert o1.relative_to(o2) == o1
        with pytest.raises(ValueError, match="is not in the subpath of"):
            o1.relative_to(o3)

    def test_asset(self):
        p = "s3"
        f = "bucket/object"
        i = Asset(uri=f"{p}://{f}", name="test-asset", extra={"foo": "bar"})
        o = ObjectStoragePath(i)
        assert o.protocol == p
        assert o.path == f

    def test_move_local(self, hook_lineage_collector, tmp_path, target):
        o1 = ObjectStoragePath(f"file://{target}")
        o2 = ObjectStoragePath(f"file://{tmp_path}/{uuid.uuid4()}")
        assert o1.exists()
        assert not o2.exists()

        o1.move(o2)
        assert o2.exists()
        assert not o1.exists()

        collected_assets = hook_lineage_collector.collected_assets
        assert len(collected_assets.inputs) == 1
        assert len(collected_assets.outputs) == 1
        assert collected_assets.inputs[0].asset.uri == str(o1)
        assert collected_assets.outputs[0].asset.uri == str(o2)

    def test_serde_objectstoragepath(self):
        path = "file:///bucket/key/part1/part2"
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

        store = attach("file", conn_id="mock")
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


class TestBackwardsCompatibility:
    @pytest.fixture(autouse=True)
    def reset(self):
        from airflow.sdk.io.fs import _register_filesystems

        _register_filesystems.cache_clear()
        yield
        _register_filesystems.cache_clear()

    def test_backwards_compat(self):
        from airflow.io import _BUILTIN_SCHEME_TO_FS, get_fs

        def get_fs_no_storage_options(_: str):
            return LocalFileSystem()

        _BUILTIN_SCHEME_TO_FS["file"] = get_fs_no_storage_options  # type: ignore[call-arg]
        assert get_fs("file")
        with pytest.raises(AttributeError):
            get_fs("file", storage_options={"foo": "bar"})
