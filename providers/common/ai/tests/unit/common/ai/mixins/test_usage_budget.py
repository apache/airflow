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
from decimal import Decimal
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pydantic_ai.usage import RunUsage, UsageLimits

from airflow.providers.common.ai.mixins.usage_budget import UsageBudgetMixin
from airflow.providers.common.ai.utils.usage_budget import TaskStateStoreUsageBudget

MIXIN_MODULE = "airflow.providers.common.ai.mixins.usage_budget"


class _Host(UsageBudgetMixin):
    def __init__(self, usage_limits=None):
        self.usage_limits = usage_limits
        self.log = logging.getLogger(__name__)
        self.run_agent_sync = MagicMock()


class TestBuildUsageBudget:
    @mock.patch(f"{MIXIN_MODULE}.AIRFLOW_V_3_3_PLUS", True)
    def test_builds_budget_from_task_state_store_and_max_tries(self):
        context = {"task_state_store": MagicMock()}
        ti = MagicMock(max_tries=3)

        budget = _Host()._build_usage_budget(context, UsageLimits(request_limit=5), ti=ti)

        assert isinstance(budget, TaskStateStoreUsageBudget)

    @mock.patch(f"{MIXIN_MODULE}.AIRFLOW_V_3_3_PLUS", False)
    def test_skipped_before_airflow_3_3(self):
        assert _Host()._build_usage_budget({}, UsageLimits(request_limit=5), ti=MagicMock()) is None

    @mock.patch(f"{MIXIN_MODULE}.AIRFLOW_V_3_3_PLUS", True)
    def test_skipped_without_usage_limits(self):
        assert _Host()._build_usage_budget({}, None, ti=None) is None


class TestGetUsageBudget:
    def test_does_not_read_task_instance_without_usage_limits(self):
        # A context without ``task_instance`` raises KeyError on access.
        assert _Host()._get_usage_budget({}, None) is None

    def test_passes_task_instance_when_usage_limits_set(self):
        host = _Host()
        context = {"task_instance": MagicMock()}
        limits = UsageLimits(request_limit=5)

        with mock.patch.object(host, "_build_usage_budget") as build:
            result = host._get_usage_budget(context, limits)

        build.assert_called_once_with(context, limits, ti=context["task_instance"])
        assert result is build.return_value


class TestRunTracked:
    def test_saves_cumulative_usage_and_returns_only_this_attempts_delta(self):
        host = _Host()
        host._usage_budget = MagicMock(spec=TaskStateStoreUsageBudget)
        run_usage = RunUsage(requests=2, input_tokens=100)

        def run(agent, prompt, *, usage, **kwargs):
            usage.requests += 1
            usage.input_tokens += 10
            return "result"

        host.run_agent_sync.side_effect = run

        result, delta = host._run_tracked(MagicMock(), "prompt", run_usage=run_usage)

        assert result == "result"
        assert (delta.requests, delta.input_tokens) == (1, 10)
        host._usage_budget.save.assert_called_once_with(run_usage)
        assert run_usage.requests == 3

    def test_saves_usage_when_run_raises(self):
        host = _Host()
        host._usage_budget = MagicMock(spec=TaskStateStoreUsageBudget)
        run_usage = RunUsage()
        host.run_agent_sync.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            host._run_tracked(MagicMock(), "prompt", run_usage=run_usage)

        host._usage_budget.save.assert_called_once_with(run_usage)

    def test_before_run_credit_is_applied_before_the_run(self):
        host = _Host()
        run_usage = RunUsage()
        seen = {}

        def credit():
            run_usage.requests += 4

        def run(agent, prompt, *, usage, **kwargs):
            seen["requests"] = usage.requests

        host.run_agent_sync.side_effect = run

        host._run_tracked(MagicMock(), "prompt", run_usage=run_usage, before_run=credit)

        assert seen["requests"] == 4

    def test_settles_before_persisting(self):
        host = _Host()
        host._usage_budget = MagicMock(spec=TaskStateStoreUsageBudget)
        order = MagicMock()
        order.attach_mock(host._usage_budget.save, "save")
        host._settle_tracked_usage = order.settle

        host._run_tracked(MagicMock(), "prompt", run_usage=RunUsage())

        assert [c[0] for c in order.mock_calls] == ["settle", "save"]

    def test_does_not_persist_without_budget(self):
        host = _Host()

        result, _ = host._run_tracked(MagicMock(), "prompt", run_usage=RunUsage())

        assert result is host.run_agent_sync.return_value


class TestLogCumulativeUsage:
    def test_logs_cost_only_when_known(self):
        host = _Host()
        host.log = MagicMock()

        host._log_cumulative_usage(RunUsage(requests=1))
        assert host.log.info.call_count == 1

        host.log.reset_mock()
        usage = RunUsage(requests=1)
        usage.cost = Decimal("0.5")
        host._log_cumulative_usage(usage)
        assert host.log.info.call_count == 2


class TestClearUsageBudget:
    def test_clears_existing_budget(self):
        host = _Host()
        host._usage_budget = MagicMock(spec=TaskStateStoreUsageBudget)

        host._clear_usage_budget({})

        host._usage_budget.clear.assert_called_once_with()

    def test_rebuilds_budget_from_usage_limits_on_resumed_instance(self):
        host = _Host(usage_limits={"request_limit": 5})
        context = {"task_instance": MagicMock()}
        budget = MagicMock(spec=TaskStateStoreUsageBudget)

        with mock.patch.object(host, "_get_usage_budget", return_value=budget) as get_budget:
            host._clear_usage_budget(context)

        assert get_budget.call_args.args[0] is context
        assert get_budget.call_args.args[1].request_limit == 5
        budget.clear.assert_called_once_with()

    def test_noop_when_no_budget_applies(self):
        host = _Host()

        with mock.patch.object(host, "_get_usage_budget", return_value=None):
            host._clear_usage_budget({})
