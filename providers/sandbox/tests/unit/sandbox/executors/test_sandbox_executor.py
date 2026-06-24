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

import queue
import threading

from airflow.providers.sandbox.backends.base import ExecResult, SandboxState
from airflow.providers.sandbox.executors.sandbox_executor import (
    _GONE_AFTER_CONSECUTIVE_UNKNOWN,
    SandboxExecutor,
    SandboxWatcher,
)


class _FlakyProvider:
    """Returns UNKNOWN a few times, then RUNNING — must never escalate to GONE."""

    def __init__(self):
        self.calls = 0

    def poll_status(self, handle, exec_ref):
        self.calls += 1
        if self.calls <= _GONE_AFTER_CONSECUTIVE_UNKNOWN - 1:
            return ExecResult(state=SandboxState.UNKNOWN)
        return ExecResult(state=SandboxState.RUNNING)


def test_transient_unknown_does_not_kill_running_task():
    key = ("dag", "task", "run", 1)
    inflight = {key: ("h1", "e1")}
    lock = threading.Lock()
    results: queue.Queue = queue.Queue()
    watcher = SandboxWatcher(_FlakyProvider(), inflight, lock, results, interval=0.01)

    for _ in range(_GONE_AFTER_CONSECUTIVE_UNKNOWN + 1):
        res = watcher.provider.poll_status("h1", "e1")
        if res.state is SandboxState.UNKNOWN:
            watcher._unknown_streak[key] += 1
            if watcher._unknown_streak[key] >= _GONE_AFTER_CONSECUTIVE_UNKNOWN:
                watcher._emit(key, ExecResult(state=SandboxState.GONE), "h1")
        else:
            watcher._unknown_streak.pop(key, None)

    assert results.empty(), "a transient UNKNOWN streak broken by RUNNING must not emit GONE"


def test_capability_flags():
    assert SandboxExecutor.is_local is False
    assert SandboxExecutor.supports_ad_hoc_ti_run is False
