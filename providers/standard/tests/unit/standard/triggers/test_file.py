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

import asyncio

import anyio
import pytest

from airflow.providers.standard.triggers.file import (
    DirectoryFileDeleteTrigger,
    FileDeleteTrigger,
    FileTrigger,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


class TestFileTrigger:
    FILE_PATH = "/files/dags/example_async_file.py"

    def test_serialization(self):
        """Asserts that the trigger correctly serializes its arguments and classpath."""
        trigger = FileTrigger(filepath=self.FILE_PATH, poll_interval=5)
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.file.FileTrigger"
        assert kwargs == {
            "filepath": self.FILE_PATH,
            "poke_interval": 5,
            "recursive": False,
        }

    @pytest.mark.asyncio
    async def test_task_file_trigger(self, tmp_path):
        """Asserts that the trigger only goes off on or after file is found"""
        tmp_dir = tmp_path / "test_dir"
        await anyio.Path(tmp_dir).mkdir()
        p = tmp_dir / "hello.txt"

        trigger = FileTrigger(
            filepath=str(p.resolve()),
            poke_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        p.touch()

        # Await the task directly so the assertion can't race the trigger's
        # detect → yield cycle on slow runners (ARM, Pendulum2 special job).
        await asyncio.wait_for(task, timeout=5.0)
        assert task.done() is True


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
class TestFileDeleteTrigger:
    FILE_PATH = "/files/dags/example_async_file.py"

    def test_serialization(self):
        """Asserts that the trigger correctly serializes its arguments and classpath."""
        trigger = FileDeleteTrigger(filepath=self.FILE_PATH, poll_interval=5)
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.file.FileDeleteTrigger"
        assert kwargs == {
            "filepath": self.FILE_PATH,
            "poke_interval": 5,
        }

    @pytest.mark.asyncio
    async def test_file_delete_trigger(self, tmp_path):
        """Asserts that the trigger goes off on or after file is found and that the files gets deleted."""
        tmp_dir = tmp_path / "test_dir"
        await anyio.Path(tmp_dir).mkdir()
        p = tmp_dir / "hello.txt"

        trigger = FileDeleteTrigger(
            filepath=str(p.resolve()),
            poke_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        p.touch()

        # Await the task directly so the assertion can't race the trigger's
        # detect → unlink → yield cycle on slow runners (ARM, Pendulum2
        # special job). The trigger only yields after `await filepath.unlink()`
        # returns, so once the task is done, the file is guaranteed gone.
        await asyncio.wait_for(task, timeout=5.0)
        assert await anyio.Path(p).exists() is False


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
class TestDirectoryFileDeleteTrigger:
    DIRECTORY = "/data/flags"

    def test_serialization(self):
        trigger = DirectoryFileDeleteTrigger(
            directory=self.DIRECTORY, filename="orders_us.flag", poke_interval=5
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.file.DirectoryFileDeleteTrigger"
        assert kwargs == {
            "directory": self.DIRECTORY,
            "filename": "orders_us.flag",
            "poke_interval": 5,
        }

    def test_shared_stream_key_groups_same_directory_and_cadence(self):
        a = DirectoryFileDeleteTrigger(directory=self.DIRECTORY, filename="us.flag", poke_interval=1.0)
        b = DirectoryFileDeleteTrigger(directory=self.DIRECTORY, filename="eu.flag", poke_interval=1.0)
        c = DirectoryFileDeleteTrigger(directory=self.DIRECTORY, filename="us.flag", poke_interval=2.0)
        d = DirectoryFileDeleteTrigger(directory="/other", filename="us.flag", poke_interval=1.0)

        assert a.shared_stream_key() == b.shared_stream_key()
        assert a.shared_stream_key() != c.shared_stream_key()
        assert a.shared_stream_key() != d.shared_stream_key()

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            ("/data/flags", "/data/flags/"),
            ("/data/flags", "/data//flags"),
            ("/data/flags", "/data/./flags"),
            ("/data/parent/../flags", "/data/flags"),
        ],
    )
    def test_shared_stream_key_normalises_trivial_path_variants(self, first, second):
        a = DirectoryFileDeleteTrigger(directory=first, filename="us.flag", poke_interval=1.0)
        b = DirectoryFileDeleteTrigger(directory=second, filename="us.flag", poke_interval=1.0)
        assert a.shared_stream_key() == b.shared_stream_key()

    def test_shared_stream_key_realpath_trailing_slash(self, tmp_path):
        """Trailing slash variant keys to the same group as the plain path."""
        real_dir = str(tmp_path / "flags")
        a = DirectoryFileDeleteTrigger(directory=real_dir, filename="f", poke_interval=1.0)
        b = DirectoryFileDeleteTrigger(directory=real_dir + "/", filename="f", poke_interval=1.0)
        assert a.shared_stream_key() == b.shared_stream_key()

    def test_shared_stream_key_realpath_relative_vs_absolute(self, tmp_path, monkeypatch):
        """A relative path resolves to the same key as its absolute equivalent."""
        monkeypatch.chdir(tmp_path)
        a = DirectoryFileDeleteTrigger(directory=".", filename="f", poke_interval=1.0)
        b = DirectoryFileDeleteTrigger(directory=str(tmp_path), filename="f", poke_interval=1.0)
        assert a.shared_stream_key() == b.shared_stream_key()

    @pytest.mark.skipif(
        not hasattr(__import__("os"), "symlink"),
        reason="symlinks not supported on this platform",
    )
    def test_shared_stream_key_realpath_symlink_vs_target(self, tmp_path):
        """A symlink and its target resolve to the same key."""

        real_dir = tmp_path / "real"
        real_dir.mkdir()
        link_dir = tmp_path / "link"
        link_dir.symlink_to(real_dir)
        a = DirectoryFileDeleteTrigger(directory=str(real_dir), filename="f", poke_interval=1.0)
        b = DirectoryFileDeleteTrigger(directory=str(link_dir), filename="f", poke_interval=1.0)
        assert a.shared_stream_key() == b.shared_stream_key()

    @pytest.mark.asyncio
    async def test_filter_shared_stream_fires_only_for_own_filename(self, tmp_path):
        directory = tmp_path / "flags"
        await anyio.Path(directory).mkdir()
        await (anyio.Path(directory) / "us.flag").touch()

        async def stream():
            yield {"directory": str(directory), "names": {"us.flag", "eu.flag"}}

        us = DirectoryFileDeleteTrigger(directory=str(directory), filename="us.flag", poke_interval=1.0)
        events = [event async for event in us.filter_shared_stream(stream())]

        assert len(events) == 1
        assert events[0].payload == {"filepath": str(directory / "us.flag")}
        assert await (anyio.Path(directory) / "us.flag").exists() is False

    @pytest.mark.asyncio
    async def test_filter_shared_stream_skips_other_filenames(self, tmp_path):
        directory = tmp_path / "flags"
        await anyio.Path(directory).mkdir()
        await (anyio.Path(directory) / "eu.flag").touch()

        async def stream():
            yield {"directory": str(directory), "names": {"eu.flag"}}

        us = DirectoryFileDeleteTrigger(directory=str(directory), filename="us.flag", poke_interval=1.0)
        events = [event async for event in us.filter_shared_stream(stream())]

        # Did not fire, did not delete the unrelated file.
        assert events == []
        assert await (anyio.Path(directory) / "eu.flag").exists() is True

    @pytest.mark.asyncio
    async def test_filter_shared_stream_recovers_when_sibling_unlinks_first(self, tmp_path):
        directory = tmp_path / "flags"
        await anyio.Path(directory).mkdir()

        async def stream():
            # Snapshot says the file is there; in reality a sibling already
            # consumed it, so unlink raises FileNotFoundError. We must keep
            # iterating, not crash. After the snapshot drops the filename,
            # we exit the iterator without firing.
            yield {"directory": str(directory), "names": {"us.flag"}}
            yield {"directory": str(directory), "names": set()}

        us = DirectoryFileDeleteTrigger(directory=str(directory), filename="us.flag", poke_interval=1.0)
        events = [event async for event in us.filter_shared_stream(stream())]

        assert events == []

    @pytest.mark.asyncio
    async def test_open_shared_stream_handles_missing_directory(self, tmp_path):
        missing = tmp_path / "does_not_exist"
        snapshots = []

        async def consume():
            it = DirectoryFileDeleteTrigger.open_shared_stream(
                {"directory": str(missing), "poke_interval": 0.01}
            ).__aiter__()
            for _ in range(2):
                snapshots.append(await it.__anext__())

        await asyncio.wait_for(consume(), timeout=1.0)

        assert all(s["names"] == set() for s in snapshots)

    @pytest.mark.parametrize(
        "exc_cls",
        [PermissionError, NotADirectoryError, IsADirectoryError],
    )
    @pytest.mark.asyncio
    async def test_open_shared_stream_raises_on_config_bug_oserror(self, mocker, tmp_path, exc_cls):
        """PermissionError, NotADirectoryError, and IsADirectoryError must propagate rather than spin."""

        async def _iterdir(self):
            raise exc_cls("config bug")
            if False:
                yield  # pragma: no cover - sentinel for async generator typing

        mocker.patch.object(anyio.Path, "iterdir", _iterdir)

        directory = tmp_path / "flags"
        gen = DirectoryFileDeleteTrigger.open_shared_stream(
            {"directory": str(directory), "poke_interval": 0.01}
        )
        with pytest.raises(exc_cls):
            await gen.__anext__()

    @pytest.mark.asyncio
    async def test_open_shared_stream_swallows_transient_oserror(self, tmp_path, mocker):
        """A generic OSError is logged and retried; the snapshot from the next call is yielded."""
        call_count = 0

        async def _iterdir(self):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("transient blip")
            if False:
                yield  # pragma: no cover - sentinel for async generator typing

        mocker.patch.object(anyio.Path, "iterdir", _iterdir)

        async def _noop_sleep(_duration):
            pass

        mocker.patch("asyncio.sleep", side_effect=_noop_sleep)

        directory = tmp_path / "flags"
        gen = DirectoryFileDeleteTrigger.open_shared_stream(
            {"directory": str(directory), "poke_interval": 0.01}
        )
        snapshot = await gen.__anext__()

        assert snapshot == {"directory": str(directory), "names": set()}
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_run_standalone_fallback_polls_until_filename_appears(self, tmp_path):
        directory = tmp_path / "flags"
        await anyio.Path(directory).mkdir()
        target = anyio.Path(directory) / "us.flag"

        trigger = DirectoryFileDeleteTrigger(directory=str(directory), filename="us.flag", poke_interval=0.05)
        task = asyncio.create_task(trigger.run().__anext__())

        await asyncio.sleep(0.2)
        assert task.done() is False

        await target.touch()
        event = await asyncio.wait_for(task, timeout=1.0)

        assert event.payload == {"filepath": str(target)}
        assert await target.exists() is False
