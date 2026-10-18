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

from multiprocessing import SimpleQueue
from unittest.mock import MagicMock

import pytest

from airflow.executors.local_executor import LocalExecutor


@pytest.fixture
def executor():
    executor = LocalExecutor(parallelism=2)
    executor.workers = {}
    executor._worker_queues = {}
    executor.result_queue = SimpleQueue()
    yield executor
    executor.result_queue.close()


def _worker(executor, pid, alive):
    proc = MagicMock()
    proc.is_alive.return_value = alive
    executor.workers[pid] = proc
    executor._worker_queues[pid] = MagicMock()
    return proc


def test_alive_workers_are_kept(executor):
    proc1 = _worker(executor, 1, alive=True)
    proc2 = _worker(executor, 2, alive=True)
    executor._check_workers()
    proc1.close.assert_not_called()
    proc2.close.assert_not_called()
    assert set(executor.workers) == {1, 2}
    assert set(executor._worker_queues) == {1, 2}


def test_dead_worker_is_reaped_with_its_queue(executor):
    dead = _worker(executor, 1, alive=False)
    alive = _worker(executor, 2, alive=True)
    executor._check_workers()
    dead.close.assert_called_once()
    alive.close.assert_not_called()
    assert executor.workers == {2: alive}
    assert set(executor._worker_queues) == {2}


def test_dead_worker_before_start_leaves_key_for_revoke(executor):
    _worker(executor, 1, alive=False)
    executor._dispatched[1] = "key"
    executor._finish_dispatch = MagicMock()
    executor._check_workers()
    assert 1 not in executor._dispatched
    executor._finish_dispatch.assert_not_called()  # scheduler's stuck-in-queued revoke releases it
