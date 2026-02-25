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

from unittest.mock import MagicMock

import pytest

from airflow.executors.local_executor import LocalExecutor


@pytest.fixture(autouse=True)
def setup_executor(monkeypatch):
    executor = LocalExecutor(parallelism=2)
    executor.workers = {}
    executor._unread_messages = MagicMock()
    executor.activity_queue = MagicMock()
    monkeypatch.setattr(executor, "_spawn_worker", MagicMock())
    return executor


def test_no_workers_on_no_work(setup_executor):
    executor = setup_executor
    executor._unread_messages.value = 0
    executor.activity_queue.empty.return_value = True
    executor._check_workers()
    executor._spawn_worker.assert_not_called()
    assert executor.workers == {}


def test_all_workers_alive(setup_executor):
    executor = setup_executor
    proc1 = MagicMock()
    proc1.is_alive.return_value = True
    proc2 = MagicMock()
    proc2.is_alive.return_value = True
    executor.workers = {1: proc1, 2: proc2}
    executor._unread_messages.value = 0
    executor.activity_queue.empty.return_value = True
    executor._check_workers()
    proc1.close.assert_not_called()
    proc2.close.assert_not_called()
    assert len(executor.workers) == 2


def test_some_workers_dead(setup_executor):
    executor = setup_executor
    proc1 = MagicMock()
    proc1.is_alive.return_value = False
    proc2 = MagicMock()
    proc2.is_alive.return_value = True
    executor.workers = {1: proc1, 2: proc2}
    executor._unread_messages.value = 0
    executor.activity_queue.empty.return_value = True
    executor._check_workers()
    proc1.close.assert_called_once()
    proc2.close.assert_not_called()
    assert executor.workers == {2: proc2}


def test_all_workers_dead(setup_executor):
    executor = setup_executor
    proc1 = MagicMock()
    proc1.is_alive.return_value = False
    proc2 = MagicMock()
    proc2.is_alive.return_value = False
    executor.workers = {1: proc1, 2: proc2}
    executor._unread_messages.value = 0
    executor.activity_queue.empty.return_value = True
    executor._check_workers()
    proc1.close.assert_called_once()
    proc2.close.assert_called_once()
    assert executor.workers == {}


def test_outstanding_messages_and_empty_queue(setup_executor):
    executor = setup_executor
    executor._unread_messages.value = 1
    executor.activity_queue.empty.return_value = True
    executor._check_workers()
    executor._spawn_worker.assert_not_called()


def test_spawn_worker_when_needed(setup_executor):
    executor = setup_executor
    executor._unread_messages.value = 1
    executor.activity_queue.empty.return_value = False
    executor.workers = {}
    executor._check_workers()
    executor._spawn_worker.assert_called()


def test_no_spawn_if_parallelism_reached(setup_executor):
    executor = setup_executor
    executor._unread_messages.value = 2
    executor.activity_queue.empty.return_value = False
    proc1 = MagicMock()
    proc1.is_alive.return_value = True
    proc2 = MagicMock()
    proc2.is_alive.return_value = True
    executor.workers = {1: proc1, 2: proc2}
    executor._check_workers()
    executor._spawn_worker.assert_not_called()


def test_spawn_worker_when_we_have_parallelism_left(setup_executor):
    executor = setup_executor
    # Simulate 4 running workers
    running_workers = {}
    for i in range(4):
        proc = MagicMock()
        proc.is_alive.return_value = True
        running_workers[i] = proc
    executor.workers = running_workers
    executor.parallelism = 5  # Allow more workers if needed

    # Simulate 4 pending tasks (equal to running workers)
    executor._unread_messages.value = 4
    executor.activity_queue.empty.return_value = False
    executor._spawn_worker.reset_mock()
    executor._check_workers()
    executor._spawn_worker.assert_called()
