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

import pytest
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.usage import RunUsage, UsageLimits

from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger


class TestReplayUsageLedgerToolCalls:
    def test_credited_replay_nets_to_zero_and_settle_returns_unused_credits(self):
        """Step 1 replays (the tool manager then adds 1); step 2 is never reached, so its
        credit comes back on settle."""
        run_usage = RunUsage(tool_calls=5)
        ledger = ReplayUsageLedger(run_usage, None)

        ledger.credit_tool_steps([1, 2])
        assert run_usage.tool_calls == 3
        ledger.record_tool_replay(1)
        run_usage.tool_calls += 1
        ledger.settle()

        assert run_usage.tool_calls == 5

    def test_uncredited_replay_is_cancelled_when_it_happens(self):
        run_usage = RunUsage(tool_calls=5)
        ledger = ReplayUsageLedger(run_usage, None)

        ledger.record_tool_replay(7)
        run_usage.tool_calls += 1

        assert run_usage.tool_calls == 5

    @pytest.mark.parametrize(
        ("tool_calls_limit", "raises"),
        [pytest.param(6, False, id="within-limit"), pytest.param(5, True, id="over-limit")],
    )
    def test_credited_call_that_runs_live_counts_once_and_is_checked_again(self, tool_calls_limit, raises):
        """The up-front projection passed against the credited count; once the call is
        live it is a real sixth call and must be checked against the limit again."""
        limits = UsageLimits(tool_calls_limit=tool_calls_limit)
        run_usage = RunUsage(tool_calls=5)
        ledger = ReplayUsageLedger(run_usage, limits)
        ledger.credit_tool_steps([1])
        projected = RunUsage(tool_calls=run_usage.tool_calls + 1)
        limits.check_before_tool_call(projected)

        if raises:
            with pytest.raises(UsageLimitExceeded, match="tool_calls_limit"):
                ledger.record_live_tool_call(1)
            assert run_usage.tool_calls == 5
        else:
            ledger.record_live_tool_call(1)
            run_usage.tool_calls += 1
            assert run_usage.tool_calls == 6
