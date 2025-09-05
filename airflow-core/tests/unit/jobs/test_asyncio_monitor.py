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
import time

import pytest

from airflow.jobs.asyncio_monitor import AsyncioStallMonitor, StallIncident


async def wait_until(pred, timeout=2.0, interval=0.01):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if pred():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.mark.asyncio
async def test_start_sets_ident_and_heartbeat():
    loop = asyncio.get_running_loop()
    mon = AsyncioStallMonitor(loop=loop, threshold=0.20, heartbeat_interval=0.05)
    try:
        mon.start()
        # let call_soon callbacks run
        await asyncio.sleep(0.05)

        assert isinstance(mon._loop_thread_ident, int)

        hb1 = mon._hb_perf
        await asyncio.sleep(0.08)
        hb2 = mon._hb_perf
        assert hb2 > hb1
    finally:
        mon.stop()


@pytest.mark.asyncio
async def test_detects_stall_and_records_history():
    loop = asyncio.get_running_loop()
    mon = AsyncioStallMonitor(
        loop=loop,
        threshold=0.08,
        heartbeat_interval=0.01,
        min_report_interval=0.02,
        max_frames=50,
    )

    async def bad_task():
        import time as _t

        _t.sleep(0.2)  # noqa: ASYNC251  -- intentional: simulate event-loop stall
        await asyncio.sleep(0)

    try:
        mon.start()
        await asyncio.sleep(0.02)  # ensure ident captured

        t = asyncio.create_task(bad_task())
        t.set_name("bad_task")
        await t

        assert await wait_until(lambda: len(mon.history) > 0, timeout=2.0)
        inc: StallIncident = mon.history[-1]

        assert inc.ended_at_perf is not None
        assert inc.samples, "Incident has no captured samples"

        combined_stack = "\n".join(s.stack_text for s in inc.samples)
        assert ("bad_task" in combined_stack) or (__file__ in combined_stack)
    finally:
        mon.stop()


@pytest.mark.asyncio
async def test_no_stall_no_history():
    loop = asyncio.get_running_loop()
    mon = AsyncioStallMonitor(loop=loop, threshold=0.5, heartbeat_interval=0.05)
    try:
        mon.start()
        await asyncio.sleep(0.3)  # cooperative only
        await asyncio.sleep(0.1)  # let watchdog tick
        assert len(mon.history) == 0
    finally:
        mon.stop()
