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

import sys
import uuid
from stat import S_ISDIR, S_ISREG
from tempfile import NamedTemporaryFile
from typing import Any, ClassVar
from unittest import mock

import pytest
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from fsspec.registry import _registry as _fsspec_registry, register_implementation

from airflow.assets import Asset
from airflow.io import _register_filesystems, get_fs
from airflow.io.path import ObjectStoragePath
from airflow.io.store import _STORE_CACHE, ObjectStore, attach
from airflow.utils.module_loading import qualname

FAKE = "file:///fake"
MNT = "file:///mnt/warehouse"
FOO = "file:///mnt/warehouse/foo"
BAR = FOO


class FakeLocalFileSystem(MemoryFileSystem):
    protocol = ("file", "local")
    root_marker = "/"
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


class FakeRemoteFileSystem(MemoryFileSystem):
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


def get_fs_no_storage_options(_: str):
    return LocalFileSystem()


class TestFs:
    def setup_class(self):
        self._store_cache = _STORE_CACHE.copy()
        self._fsspec_registry = _fsspec_registry.copy()
        for protocol in FakeRemoteFileSystem.protocol:
            register_implementation(protocol, FakeRemoteFileSystem, clobber=True)

    def teardown(self):
        _STORE_CACHE.clear()
        _STORE_CACHE.update(self._store_cache)
        _fsspec_registry.clear()
        _fsspec_registry.update(self._fsspec_registry)

    def test_alias(self):
        store = attach("file", alias="local")
        assert isinstance(store.fs, LocalFileSystem)
        assert "local" in _STORE_CACHE

    def test_init_objectstoragepath(self):
        attach("s3", fs=FakeRemoteFileSystem())

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

    def test_objectstoragepath_init_conn_id_in_uri(self):
        attach(protocol="fake", conn_id="fake", fs=FakeRemoteFileSystem(conn_id="fake"))
        p = ObjectStoragePath("fake://fake@bucket/path")
        p.touch()
        fsspec_info = p.fs.info(p.path)
        assert p.stat() == {**fsspec_info, "conn_id": "fake", "protocol": "fake"}

    @pytest.fixture
    def fake_local_files(self):
        obj = FakeLocalFileSystem()
        obj.touch(FOO)
        try:
            yield
        finally:
            FakeLocalFileSystem.store.clear()
            FakeLocalFileSystem.pseudo_dirs[:] = [""]

    @pytest.mark.parametrize(
        "fn, args, fn2, path, expected_args, expected_kwargs",
        [
            (
                "checksum",
                {},
                "checksum",
                FOO,
                FakeLocalFileSystem._strip_protocol(BAR),
                {},
            ),
            ("size", {}, "size", FOO, FakeLocalFileSystem._strip_protocol(BAR), {}),
            (
                "sign",
                {"expiration": 200, "extra": "xtra"},
                "sign",
                FOO,
                FakeLocalFileSystem._strip_protocol(BAR),
                {"expiration": 200, "extra": "xtra"},
            ),
            ("ukey", {}, "ukey", FOO, FakeLocalFileSystem._strip_protocol(BAR), {}),
            (
                "read_block",
                {"offset": 0, "length": 1},
                "read_block",
                FOO,
                FakeLocalFileSystem._strip_protocol(BAR),
                {"delimiter": None, "length": 1, "offset": 0},
            ),
        ],
    )
    def test_standard_extended_api(
        self, fake_local_files, fn, args, fn2, path, expected_args, expected_kwargs
    ):
        fs = FakeLocalFileSystem()
        with mock.patch.object(fs, fn2) as method:
            attach(protocol="file", conn_id="fake", fs=fs)
            o = ObjectStoragePath(path, conn_id="fake")

            getattr(o, fn)(**args)
            method.assert_called_once_with(expected_args, **expected_kwargs)

    def test_stat(self):
        with NamedTemporaryFile() as f:
            o = ObjectStoragePath(f"file://{f.name}")
            assert o.stat().st_size == 0
            assert S_ISREG(o.stat().st_mode)
            assert S_ISDIR(o.parent.stat().st_mode)

    def test_bucket_key_protocol(self):
        attach(protocol="s3", fs=FakeRemoteFileSystem())

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

    @pytest.mark.skipif(
        sys.version_info < (3, 9), reason="`is_relative_to` new in version 3.9"
    )
    def test_is_relative_to(self):
        uuid_dir = f"/tmp/{str(uuid.uuid4())}"
        o1 = ObjectStoragePath(f"file://{uuid_dir}/aaa")
        o2 = ObjectStoragePath(f"file://{uuid_dir}")
        o3 = ObjectStoragePath(f"file://{str(uuid.uuid4())}")
        assert o1.is_relative_to(o2)
        assert not o1.is_relative_to(o3)

    def test_relative_to(self):
        uuid_dir = f"/tmp/{str(uuid.uuid4())}"
        o1 = ObjectStoragePath(f"file://{uuid_dir}/aaa")
        o2 = ObjectStoragePath(f"file://{uuid_dir}")
        o3 = ObjectStoragePath(f"file://{str(uuid.uuid4())}")

        _ = o1.relative_to(o2)  # Should not raise any error

        with pytest.raises(ValueError):
            o1.relative_to(o3)

    def test_move_local(self, hook_lineage_collector):
        _from_path = f"file:///tmp/{str(uuid.uuid4())}"
        _to_path = f"file:///tmp/{str(uuid.uuid4())}"
        _from = ObjectStoragePath(_from_path)
        _to = ObjectStoragePath(_to_path)

        _from.touch()
        _from.move(_to)
        assert _to.exists()
        assert not _from.exists()

        _to.unlink()

        collected_assets = hook_lineage_collector.collected_assets

        assert len(collected_assets.inputs) == 1
        assert len(collected_assets.outputs) == 1
        assert collected_assets.inputs[0].asset == Asset(uri=_from_path)
        assert collected_assets.outputs[0].asset == Asset(uri=_to_path)

    def test_move_remote(self, hook_lineage_collector):
        attach("fakefs", fs=FakeRemoteFileSystem())

        _from_path = f"file:///tmp/{str(uuid.uuid4())}"
        _to_path = f"fakefs:///tmp/{str(uuid.uuid4())}"

        _from = ObjectStoragePath(_from_path)
        _to = ObjectStoragePath(_to_path)

        _from.touch()
        _from.move(_to)
        assert not _from.exists()
        assert _to.exists()

        _to.unlink()

        collected_assets = hook_lineage_collector.collected_assets

        assert len(collected_assets.inputs) == 1
        assert len(collected_assets.outputs) == 1
        assert collected_assets.inputs[0].asset == Asset(uri=str(_from))
        assert collected_assets.outputs[0].asset == Asset(uri=str(_to))

    def test_copy_remote_remote(self, hook_lineage_collector):
        attach("ffs", fs=FakeRemoteFileSystem(skip_instance_cache=True))
        attach("ffs2", fs=FakeRemoteFileSystem(skip_instance_cache=True))

        dir_src = f"bucket1/{str(uuid.uuid4())}"
        dir_dst = f"bucket2/{str(uuid.uuid4())}"
        key = "foo/bar/baz.txt"

        _from_path = f"ffs://{dir_src}"
        _from = ObjectStoragePath(_from_path)
        _from_file = _from / key
        _from_file.touch()
        assert _from.bucket == "bucket1"
        assert _from_file.exists()

        _to_path = f"ffs2://{dir_dst}"
        _to = ObjectStoragePath(_to_path)
        _from.copy(_to)

        assert _to.bucket == "bucket2"
        assert _to.exists()
        assert _to.is_dir()
        assert (_to / _from.key / key).exists()
        assert (_to / _from.key / key).is_file()

        _from.rmdir(recursive=True)
        _to.rmdir(recursive=True)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=str(_from_file)
        )

        # Empty file - shutil.copyfileobj does nothing
        assert len(hook_lineage_collector.collected_assets.outputs) == 0

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

    def test_asset(self):
        attach("s3", fs=FakeRemoteFileSystem())

        p = "s3"
        f = "bucket/object"
        i = Asset(uri=f"{p}://{f}", extra={"foo": "bar"})
        o = ObjectStoragePath(i)
        assert o.protocol == p
        assert o.path == f

    def test_hash(self):
        file_uri_1 = f"file:///tmp/{str(uuid.uuid4())}"
        file_uri_2 = f"file:///tmp/{str(uuid.uuid4())}"
        s = set()
        for _ in range(10):
            s.add(ObjectStoragePath(file_uri_1))
            s.add(ObjectStoragePath(file_uri_2))
        assert len(s) == 2

    def test_lazy_load(self):
        o = ObjectStoragePath("file:///tmp/foo")
        with pytest.raises(AttributeError):
            assert o._fs_cached

        assert o.fs is not None
        assert o._fs_cached

    @pytest.mark.parametrize(
        "input_str", ("file:///tmp/foo", "s3://conn_id@bucket/test.txt")
    )
    def test_str(self, input_str):
        o = ObjectStoragePath(input_str)
        assert str(o) == input_str
