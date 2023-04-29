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

import types
from typing import Callable

from airflow import AirflowException
from airflow.decorators import python_task
from airflow.decorators.task_group import _TaskGroupFactory
from airflow.settings import _ENABLE_AIP_52


def setup_task(func: Callable) -> Callable:
    if not _ENABLE_AIP_52:
        raise AirflowException("AIP-52 Setup tasks are disabled.")

    # Using FunctionType here since _TaskDecorator is also a callable
    if isinstance(func, types.FunctionType):
        func = python_task(func)
    if isinstance(func, _TaskGroupFactory):
        raise AirflowException("Task groups cannot be marked as setup or teardown.")
    func._is_setup = True  # type: ignore[attr-defined]
    return func


def teardown_task(_func=None, *, on_failure_fail_dagrun: bool = False) -> Callable:
    if not _ENABLE_AIP_52:
        raise AirflowException("AIP-52 Teardown tasks are disabled.")

    def teardown(func: Callable) -> Callable:
        # Using FunctionType here since _TaskDecorator is also a callable
        if isinstance(func, types.FunctionType):
            func = python_task(func)
        if isinstance(func, _TaskGroupFactory):
            raise AirflowException("Task groups cannot be marked as setup or teardown.")
        func._is_teardown = True  # type: ignore[attr-defined]
        func._on_failure_fail_dagrun = on_failure_fail_dagrun  # type: ignore[attr-defined]
        return func

    if _func is None:
        return teardown
    return teardown(_func)
