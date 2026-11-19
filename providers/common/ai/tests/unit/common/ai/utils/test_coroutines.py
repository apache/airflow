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
import threading
from contextvars import ContextVar

from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync

_value: ContextVar[str] = ContextVar("value", default="unset")


async def _thread_name() -> str:
    return threading.current_thread().name


async def _read_value() -> str:
    return _value.get()


class TestRunCoroutineSync:
    def test_runs_on_the_calling_thread_when_no_loop_is_running(self):
        assert run_coroutine_sync(_thread_name()) == threading.current_thread().name

    def test_runs_on_another_thread_when_called_from_a_running_loop(self):
        async def caller() -> str:
            return run_coroutine_sync(_thread_name())

        assert asyncio.run(caller()) != threading.current_thread().name

    def test_sees_the_callers_context_variables_from_a_running_loop(self):
        async def caller() -> str:
            _value.set("caller")
            return run_coroutine_sync(_read_value())

        assert asyncio.run(caller()) == "caller"
