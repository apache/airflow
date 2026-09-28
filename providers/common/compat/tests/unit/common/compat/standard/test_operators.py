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

import functools

import pytest

from airflow.providers.common.compat.standard import operators

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

EXPECTED_EXPORTS = (
    "BaseAsyncOperator",
    "BaseBranchOperator",
    "BaseOperator",
    "BranchMixIn",
    "PythonOperator",
    "ShortCircuitOperator",
    "_SERIALIZERS",
    "get_current_context",
    "is_async_callable",
)


async def async_function():
    """Sample coroutine function."""


def sync_function():
    """Sample plain function."""


def test_public_exports():
    assert set(operators.__all__) == set(EXPECTED_EXPORTS)


@pytest.mark.parametrize("name", EXPECTED_EXPORTS)
def test_all_compat_imports_work(name):
    assert getattr(operators, name) is not None


@pytest.mark.parametrize(
    ("func", "expected"),
    [
        pytest.param(async_function, True, id="coroutine-function"),
        pytest.param(sync_function, False, id="plain-function"),
        pytest.param(functools.partial(async_function), True, id="partial-of-coroutine-function"),
        pytest.param(functools.partial(sync_function), False, id="partial-of-plain-function"),
        pytest.param(
            functools.partial(functools.partial(async_function)),
            True,
            id="nested-partial-of-coroutine-function",
        ),
    ],
)
def test_is_async_callable(func, expected):
    """
    Coroutine functions are detected through any number of ``functools.partial`` wrappers.

    These cases hold on both sides of the Airflow 3.2 fork: the local stub unwraps partials in a
    loop, and the real implementation does the same through ``unwrap_callable``.
    """
    assert operators.is_async_callable(func) is expected


@pytest.mark.skipif(AIRFLOW_V_3_2_PLUS, reason="The BaseAsyncOperator stub only exists on Airflow < 3.2")
class TestBaseAsyncOperatorStub:
    def test_is_async(self):
        operator = operators.BaseAsyncOperator(task_id="test_async")
        assert operator.is_async is True

    def test_execute_raises_runtime_error(self):
        operator = operators.BaseAsyncOperator(task_id="test_async")
        with pytest.raises(RuntimeError, match="Async operators require Airflow 3.2"):
            operator.execute(None)
