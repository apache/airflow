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

from types import SimpleNamespace

import pytest

from airflow.models.taskinstance import TaskInstance, _maybe_refund_infra_attempt
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars


class _FakeTI:
    def __init__(self, max_tries: int):
        self.max_tries = max_tries

    def __str__(self) -> str:
        return "<FakeTI>"


class _FakeTask:
    def __init__(self, retries: int):
        self.retries = retries


ENABLED = {("core", "infra_failure_refund_retries"): "True", ("core", "max_infra_refunds"): "3"}


class TestMaybeRefundInfraAttempt:
    """The single safety gate: only ``failure_kind == "infra"``, with the flag on and under the cap, refunds."""

    @conf_vars(ENABLED)
    def test_infra_failure_refunds_one_attempt(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.max_tries == 2

    @conf_vars(ENABLED)
    def test_app_failure_does_not_refund(self):
        # failure_kind=None is the ordinary worker-exception path — a real bug must spend the budget.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=None) is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    @pytest.mark.parametrize("failure_kind", ["application", "manual", "timeout"])
    def test_non_infra_kind_does_not_refund(self, failure_kind):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind=failure_kind) is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_none_retries_refunds_without_crashing(self):
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=None)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_zero_retries_still_refunds(self):
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=0)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is True
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_zero_retries_refunds_are_capped(self):
        ti, task = _FakeTI(max_tries=0), _FakeTask(retries=0)
        assert [
            _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") for _ in range(5)
        ] == [True, True, True, False, False]
        assert ti.max_tries == 3

    @conf_vars({("core", "infra_failure_refund_retries"): "False"})
    def test_disabled_by_default_does_not_refund(self):
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") is False
        assert ti.max_tries == 1

    @conf_vars(ENABLED)
    def test_cap_bounds_the_refunds(self):
        # retries=1, cap=3 → refunds allowed only while (max_tries - retries) < 3, i.e. max_tries in {1,2,3}.
        ti, task = _FakeTI(max_tries=1), _FakeTask(retries=1)
        assert [
            _maybe_refund_infra_attempt(task_instance=ti, task=task, failure_kind="infra") for _ in range(5)
        ] == [
            True,
            True,
            True,
            False,
            False,
        ]
        assert ti.max_tries == 4  # 1 + three refunds, then capped


class TestIsEligibleToRetryUsesMaxTries:
    """is_eligible_to_retry gates on max_tries, not task.retries, so an infra refund that bumps
    max_tries grants a retry even at retries=0 (and the two retry-decision paths stay in sync)."""

    @staticmethod
    def _eligible(*, max_tries: int, try_number: int, state=None) -> bool:
        stub = SimpleNamespace(state=state, max_tries=max_tries, try_number=try_number)
        return TaskInstance.is_eligible_to_retry(stub)  # type: ignore[arg-type]

    def test_zero_budget_not_bumped_is_not_eligible(self):
        assert self._eligible(max_tries=0, try_number=1) is False

    def test_zero_budget_bumped_by_refund_is_eligible(self):
        assert self._eligible(max_tries=1, try_number=1) is True

    def test_normal_budget_exhausted_is_not_eligible(self):
        assert self._eligible(max_tries=2, try_number=3) is False

    def test_normal_budget_remaining_is_eligible(self):
        assert self._eligible(max_tries=2, try_number=2) is True

    def test_restarting_always_eligible(self):
        assert self._eligible(max_tries=0, try_number=9, state=TaskInstanceState.RESTARTING) is True
