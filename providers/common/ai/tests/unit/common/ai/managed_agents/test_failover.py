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

from unittest import mock

import pytest

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError, ManagedAgentRejected
from airflow.providers.common.ai.managed_agents import (
    FailoverManagedAgentClient,
    ManagedAgentCapabilities,
    ManagedAgentClient,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
)


class FakeClient:
    def __init__(
        self,
        name: str,
        *,
        platform: str = "fake.cloud",
        raises: Exception | None = None,
        ref_raises: Exception | None = None,
        **capabilities: bool,
    ):
        self._name = name
        self._platform = platform
        self._raises = raises
        self._ref_raises = ref_raises
        self.capabilities = ManagedAgentCapabilities(**capabilities)
        self.calls = 0

    @property
    def ref(self) -> ManagedAgentRef:
        if self._ref_raises is not None:
            raise self._ref_raises
        return ManagedAgentRef(platform=self._platform, name=self._name)

    def invoke(self, request: ManagedAgentRequest) -> ManagedAgentResponse:
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        return ManagedAgentResponse(text=f"{self._name} answered", raw={})


REQUEST = ManagedAgentRequest(prompt="q")


class TestConstruction:
    @pytest.mark.parametrize("count", [0, 1], ids=["none", "one"])
    def test_needs_at_least_two_members(self, count):
        with pytest.raises(ValueError, match="at least two members"):
            FailoverManagedAgentClient([FakeClient("a")] * count)

    def test_members_are_copied_so_the_caller_cannot_empty_the_group(self):
        members = [FakeClient("a"), FakeClient("b")]
        group = FailoverManagedAgentClient(members)
        members.clear()
        assert len(group.members) == 2

    def test_session_capable_members_are_accepted_but_the_group_never_offers_sessions(self):
        group = FailoverManagedAgentClient([FakeClient("a", sessions=True), FakeClient("b", sessions=True)])
        assert group.capabilities.sessions is False

    def test_is_itself_a_client_so_groups_nest(self):
        inner = FailoverManagedAgentClient([FakeClient("a"), FakeClient("b")])
        outer = FailoverManagedAgentClient([inner, FakeClient("c")])
        assert isinstance(inner, ManagedAgentClient)
        assert outer.invoke(REQUEST).text == "a answered"


class TestIdentityAndCapabilities:
    def test_ref_names_every_member_in_order(self):
        group = FailoverManagedAgentClient([FakeClient("a"), FakeClient("b", platform="other.cloud")])
        assert group.ref == ManagedAgentRef(platform="failover", name="a on fake.cloud -> b on other.cloud")

    def test_ref_survives_a_member_whose_identity_cannot_be_resolved(self):
        group = FailoverManagedAgentClient(
            [FakeClient("a"), FakeClient("b", ref_raises=RuntimeError("Connection 'b' not found"))]
        )
        assert group.ref.name == "a on fake.cloud -> <unresolved: Connection 'b' not found>"

    def test_capabilities_are_the_intersection(self):
        group = FailoverManagedAgentClient(
            [FakeClient("a", usage=True, trace=True), FakeClient("b", usage=True)],
        )
        assert group.capabilities == ManagedAgentCapabilities(usage=True)

    def test_a_session_bound_request_is_refused_before_any_member_is_tried(self):
        primary, standby = FakeClient("a", sessions=True), FakeClient("b", sessions=True)
        with pytest.raises(ValueError, match="cannot continue a conversation"):
            FailoverManagedAgentClient([primary, standby]).invoke(
                ManagedAgentRequest(prompt="q", session_id="t:1")
            )
        assert (primary.calls, standby.calls) == (0, 0)


class TestInvoke:
    def test_primary_answer_wins_and_standby_is_untouched(self):
        primary, standby = FakeClient("a"), FakeClient("b")
        assert FailoverManagedAgentClient([primary, standby]).invoke(REQUEST).text == "a answered"
        assert (primary.calls, standby.calls) == (1, 0)

    @pytest.mark.parametrize(
        "error",
        [ManagedAgentInvocationError("down"), RuntimeError("503"), TimeoutError("read timed out")],
        ids=["terminal", "transient", "timeout"],
    )
    def test_fails_over_when_primary_fails(self, error):
        standby = FakeClient("b")
        group = FailoverManagedAgentClient([FakeClient("a", raises=error), standby])
        assert group.invoke(REQUEST).text == "b answered"
        assert standby.calls == 1

    def test_a_broken_primary_identity_does_not_prevent_failover(self):
        # The defect this design fixes: identity used to be resolved before the call, so a
        # standby with a misconfigured connection failed the whole tool call.
        primary = FakeClient("a", raises=RuntimeError("503"), ref_raises=RuntimeError("no connection"))
        standby = FakeClient("b", ref_raises=RuntimeError("no connection either"))
        assert FailoverManagedAgentClient([primary, standby]).invoke(REQUEST).text == "b answered"

    def test_rejection_does_not_burn_the_standby(self):
        standby = FakeClient("b")
        group = FailoverManagedAgentClient([FakeClient("a", raises=ManagedAgentRejected("vague")), standby])
        with pytest.raises(ManagedAgentRejected, match="vague"):
            group.invoke(REQUEST)
        assert standby.calls == 0

    def test_last_members_error_propagates_when_all_fail(self):
        group = FailoverManagedAgentClient(
            [FakeClient("a", raises=RuntimeError("a down")), FakeClient("b", raises=RuntimeError("b down"))]
        )
        with pytest.raises(RuntimeError, match="b down"):
            group.invoke(REQUEST)

    def test_narrowed_failover_on_lets_other_errors_through(self):
        standby = FakeClient("b")
        group = FailoverManagedAgentClient(
            [FakeClient("a", raises=RuntimeError("not covered")), standby], failover_on=(TimeoutError,)
        )
        with pytest.raises(RuntimeError, match="not covered"):
            group.invoke(REQUEST)
        assert standby.calls == 0

    def test_warns_on_failover_and_names_both_members(self, caplog):
        group = FailoverManagedAgentClient([FakeClient("a", raises=RuntimeError("503")), FakeClient("b")])
        with caplog.at_level("INFO"):
            group.invoke(REQUEST)
        assert "a on fake.cloud (member 0) failed; failing over to b on fake.cloud (member 1)" in caplog.text
        assert "served by standby b on fake.cloud (member 1)" in caplog.text


class TestMetrics:
    """A failover is a success-shaped event, so the counter is the only signal that a primary is down."""

    @mock.patch("airflow.providers.common.ai.managed_agents.failover.Stats.incr", autospec=True)
    def test_primary_success_emits_nothing_here(self, mock_incr):
        # The toolset counts answers; the group counts only transitions.
        FailoverManagedAgentClient([FakeClient("a"), FakeClient("b")]).invoke(REQUEST)
        mock_incr.assert_not_called()

    @mock.patch("airflow.providers.common.ai.managed_agents.failover.Stats.incr", autospec=True)
    def test_failover_is_counted_by_platform_pair(self, mock_incr):
        group = FailoverManagedAgentClient(
            [FakeClient("a", raises=RuntimeError("503")), FakeClient("b", platform="other.cloud")]
        )
        group.invoke(REQUEST)
        mock_incr.assert_called_once_with(
            "managed_agent.failover", tags={"from_platform": "fake.cloud", "to_platform": "other.cloud"}
        )

    @mock.patch("airflow.providers.common.ai.managed_agents.failover.Stats.incr", autospec=True)
    def test_unresolvable_identity_tags_as_unknown(self, mock_incr):
        group = FailoverManagedAgentClient(
            [FakeClient("a", raises=RuntimeError("503"), ref_raises=RuntimeError("x")), FakeClient("b")]
        )
        group.invoke(REQUEST)
        assert mock_incr.call_args.kwargs["tags"]["from_platform"] == "unknown"

    @mock.patch("airflow.providers.common.ai.managed_agents.failover.Stats.incr", autospec=True)
    def test_rejection_is_not_a_failover(self, mock_incr):
        group = FailoverManagedAgentClient(
            [FakeClient("a", raises=ManagedAgentRejected("x")), FakeClient("b")]
        )
        with pytest.raises(ManagedAgentRejected):
            group.invoke(REQUEST)
        mock_incr.assert_not_called()
