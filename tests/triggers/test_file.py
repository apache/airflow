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

import pytest

from airflow.triggers.file import FileTrigger


class TestFileTrigger:
    FILE_PATH = "/files/dags/example_async_file.py"

    def test_serialization(self):
        """Asserts that the trigger correctly serializes its arguments and classpath."""
        trigger = FileTrigger(filepath=self.FILE_PATH, poll_interval=5)
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.triggers.file.FileTrigger"
        assert kwargs == {
            "filepath": self.FILE_PATH,
            "poll_interval": 5,
            "recursive": False,
        }

    @pytest.mark.asyncio
    async def test_task_file_trigger(self, tmp_path):
        """Asserts that the trigger only goes off on or after file is found"""
        tmp_dir = tmp_path / "test_dir"
        tmp_dir.mkdir()
        p = tmp_dir / "hello.txt"

        trigger = FileTrigger(
            filepath=str(p.resolve()),
            poll_interval=0.2,
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
