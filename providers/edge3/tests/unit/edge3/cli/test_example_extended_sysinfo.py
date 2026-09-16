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

import logging
import sys
from unittest import mock

import pytest

from airflow.providers.edge3.cli.example_extended_sysinfo import get_example_extended_sysinfo

pytestmark = [pytest.mark.asyncio]

MODULE = "airflow.providers.edge3.cli.example_extended_sysinfo"

GIB = 1024**3


class FakeAsyncPath:
    """Stand-in for ``anyio.Path`` limited to what the sysinfo function uses."""

    def __init__(self, exists: bool = False, text: str = ""):
        self._exists = exists
        self._text = text

    async def exists(self) -> bool:
        return self._exists

    async def read_text(self) -> str:
        return self._text


def _patch_system(cpu_usage: float, disk_free_gb: float, loadavg: float = 1.55):
    return (
        mock.patch(f"{MODULE}.shutil.disk_usage", return_value=mock.Mock(free=disk_free_gb * GIB)),
        mock.patch(f"{MODULE}.psutil.cpu_percent", return_value=cpu_usage),
        mock.patch(f"{MODULE}.os.getloadavg", return_value=(loadavg, 1.0, 0.5)),
        mock.patch(f"{MODULE}.Path", side_effect=lambda path: FakeAsyncPath(exists=False)),
    )


async def test_reports_system_measurements():
    patches = _patch_system(cpu_usage=10.0, disk_free_gb=100.0, loadavg=1.554)
    with patches[0], patches[1], patches[2], patches[3]:
        sysinfo = await get_example_extended_sysinfo()

    assert sysinfo["platform"] == sys.platform
    assert sysinfo["disk_free_gb"] == 100.0
    assert sysinfo["cpu_usage"] == 10.0
    assert sysinfo["sys_load"] == 1.55


@pytest.mark.parametrize(
    ("cpu_usage", "disk_free_gb", "expected_status", "expected_text"),
    [
        (10.0, 100.0, logging.INFO, "I am good, sun is shining 🌞"),
        (71.0, 100.0, logging.WARNING, "Warning condition!"),
        (10.0, 19.0, logging.WARNING, "Warning condition!"),
        (96.0, 100.0, logging.ERROR, "Critical condition!"),
        (10.0, 4.0, logging.ERROR, "Critical condition!"),
    ],
)
async def test_status_reflects_cpu_and_disk_thresholds(
    cpu_usage, disk_free_gb, expected_status, expected_text
):
    patches = _patch_system(cpu_usage=cpu_usage, disk_free_gb=disk_free_gb)
    with patches[0], patches[1], patches[2], patches[3]:
        sysinfo = await get_example_extended_sysinfo()

    assert sysinfo["status"] == expected_status
    assert sysinfo["status_text"] == expected_text


async def test_status_file_overrides_measured_status():
    fake_paths = {
        "/tmp/edge_error_status": FakeAsyncPath(exists=True, text="40"),
        "/tmp/edge_error_status_text": FakeAsyncPath(exists=True, text="mocked outage"),
    }
    patches = _patch_system(cpu_usage=10.0, disk_free_gb=100.0)
    with (
        patches[0],
        patches[1],
        patches[2],
        mock.patch(f"{MODULE}.Path", side_effect=fake_paths.__getitem__),
    ):
        sysinfo = await get_example_extended_sysinfo()

    assert sysinfo["status"] == 40
    assert sysinfo["status_text"] == "mocked outage"


async def test_status_file_without_text_file_drops_status_text():
    fake_paths = {
        "/tmp/edge_error_status": FakeAsyncPath(exists=True, text="40"),
        "/tmp/edge_error_status_text": FakeAsyncPath(exists=False),
    }
    patches = _patch_system(cpu_usage=10.0, disk_free_gb=100.0)
    with (
        patches[0],
        patches[1],
        patches[2],
        mock.patch(f"{MODULE}.Path", side_effect=fake_paths.__getitem__),
    ):
        sysinfo = await get_example_extended_sysinfo()

    assert sysinfo["status"] == 40
    assert "status_text" not in sysinfo
