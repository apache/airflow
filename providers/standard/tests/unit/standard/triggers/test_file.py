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

from airflow.providers.standard.triggers.file import FileDeleteTrigger, FileTrigger

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

        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()


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

        await asyncio.sleep(0.5)
        assert await anyio.Path(p).exists() is False

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()
