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
from collections.abc import Sequence

from airflow.providers.common.ai.exceptions import ManagedAgentRejected
from airflow.providers.common.ai.managed_agents.contract import (
    ManagedAgentCapabilities,
    ManagedAgentClient,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
    describe,
)
from airflow.providers.common.compat.sdk import Stats

log = logging.getLogger(__name__)


class FailoverManagedAgentClient:
    """
    Active/passive failover across interchangeable managed agents.

    Members are tried in order and the first answer wins. The group is itself a
    :class:`~airflow.providers.common.ai.managed_agents.contract.ManagedAgentClient`, so
    a toolset built over it presents a single tool and the calling model has no say in
    which provider serves the request: the policy stays deterministic Python rather than a
    prompt instruction a model may ignore. Groups nest.

    Members must be *substitutable*: the same agent deployed twice, not two specialists over
    different data. Two containerized agents built from one image qualify; an agent bound to
    one platform's own objects does not, because there is nothing equivalent to fail over to.
    The group cannot check this.

    What it can check is conversation state. A failover starts a fresh conversation on the
    standby, which is correct for a one-shot consultation and wrong for a multi-turn one, so
    the group never reports ``capabilities.sessions`` and refuses a request that carries a
    ``session_id``, whatever its members support.

    :param members: Interchangeable clients, tried in order. At least two.
    :param failover_on: Exception types that move to the next member. Defaults to
        ``Exception`` because ``common.ai`` cannot enumerate the cloud SDKs' exception trees.
        Narrow it when the members' exception types are known.
        :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected` never
        triggers failover, whatever this is set to: the standby would reject the same prompt.
    """

    def __init__(
        self,
        members: Sequence[ManagedAgentClient],
        *,
        failover_on: tuple[type[BaseException], ...] = (Exception,),
    ) -> None:
        if len(members) < 2:
            raise ValueError(
                f"A failover group needs at least two members; got {len(members)}. "
                "Use the member directly instead."
            )
        # Copied, not aliased: a caller holding the original list could otherwise empty it.
        self._members = tuple(members)
        self._failover_on = failover_on

    @property
    def members(self) -> tuple[ManagedAgentClient, ...]:
        return self._members

    @property
    def ref(self) -> ManagedAgentRef:
        return ManagedAgentRef(platform="failover", name=" -> ".join(describe(m) for m in self._members))

    @property
    def capabilities(self) -> ManagedAgentCapabilities:
        """The intersection of the members', except sessions, which a group never offers."""
        caps = [m.capabilities for m in self._members]
        return ManagedAgentCapabilities(
            sessions=False,
            structured_output=all(c.structured_output for c in caps),
            usage=all(c.usage for c in caps),
            trace=all(c.trace for c in caps),
        )

    def invoke(self, request: ManagedAgentRequest) -> ManagedAgentResponse:
        if request.session_id is not None:
            raise ValueError(
                "A failover group cannot continue a conversation: the standby would start a fresh one. "
                "Send session-bound requests to one member directly."
            )
        *standbys_first, last_member = self._members
        for position, member in enumerate(standbys_first):
            try:
                response = member.invoke(request)
            except ManagedAgentRejected:
                raise
            except self._failover_on:
                standby = self._members[position + 1]
                log.warning(
                    "Managed agent %s (member %d) failed; failing over to %s (member %d)",
                    describe(member),
                    position,
                    describe(standby),
                    position + 1,
                    exc_info=True,
                )
                # A failover is a success-shaped event: without a counter, a primary that has
                # been down for a week looks identical to a healthy one. Tagged by platform
                # rather than agent name to keep cardinality bounded.
                Stats.incr(
                    "managed_agent.failover",
                    tags={"from_platform": _platform(member), "to_platform": _platform(standby)},
                )
                continue
            if position > 0:
                log.info("Managed agent request served by standby %s (member %d)", describe(member), position)
            return response
        # The last member gets no failover: whatever it raises is the group's answer.
        response = last_member.invoke(request)
        log.info(
            "Managed agent request served by standby %s (member %d)",
            describe(last_member),
            len(self._members) - 1,
        )
        return response


def _platform(client: ManagedAgentClient) -> str:
    try:
        return client.ref.platform
    except Exception:
        return "unknown"
