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
"""Vendor-neutral contract for running a vendor's own agent loop in-process."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from airflow.providers.common.ai.tools import AirflowTool


class HarnessRunError(RuntimeError):
    """
    Raised when a harness run cannot be reported as a result.

    Covers the result the vendor reported as a failure (:attr:`HarnessResult.is_error`)
    and the case where the vendor's stream ended without a result at all. The message
    has already been through Airflow's secret masker.
    """

    def __init__(self, message: str, *, subtype: str | None = None, session_id: str | None = None) -> None:
        super().__init__(message)
        self.subtype = subtype
        self.session_id = session_id


@dataclass(frozen=True)
class HarnessRequest:
    """
    What one harness run should be given.

    A frozen dataclass rather than keyword arguments, so a later version can add a
    field -- a session to resume, a budget to enforce -- with a default, and every
    existing backend and caller keeps working unchanged. This version adds none of
    those; an unimplemented capability is left out entirely rather than given a
    field nobody honors, so a Dag author is never led to believe it is supported.

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier. Overrides the model stored in the
        connection's extra field.
    :param system_prompt: System-level instructions for the agent.
    :param max_turns: Maximum number of agent turns. ``None`` leaves it to the
        backend's own default.
    :param tools: Airflow tools the agent may call.
    """

    prompt: str
    llm_conn_id: str
    model_id: str | None = None
    system_prompt: str | None = None
    max_turns: int | None = None
    tools: Sequence[AirflowTool] = ()


@dataclass(frozen=True)
class HarnessResult:
    """
    What one harness run produced.

    :param output: The agent's final text output, unmasked -- as ``AgentOperator``
        returns its output unchanged, not through the tool-result masker.
    :param is_error: Whether the vendor reported the run itself as a failure. A
        backend returns this rather than raising, so an attempt that failed can
        still report what it spent before the operator fails the task.
    :param subtype: The vendor's own classification of how the run ended, carried
        through unchanged.
    :param session_id: The vendor's session identifier, when it has one.
    :param num_turns: Number of agent turns the run took.
    :param cost_usd: Best-effort cost of the run in USD, as the vendor reports it.
    :param usage: The vendor's own usage accounting, as reported by the vendor.
    """

    output: str | None
    is_error: bool
    subtype: str | None = None
    session_id: str | None = None
    num_turns: int | None = None
    cost_usd: float | None = None
    usage: Mapping[str, Any] | None = None


class HarnessBackend(ABC):
    """
    Contract for running a vendor's own agent loop and returning its result.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Implementations must be cheap to construct, because an instance may be built at
    Dag-parse time (:class:`~airflow.providers.common.ai.operators.harness.HarnessOperator`
    builds its default backend lazily instead, but a Dag author may pass one directly
    as a module-level object): resolve credentials inside :meth:`run`, on first use.

    A run that the vendor itself reports as a failure is not raised -- it comes back as
    :class:`HarnessResult` with ``is_error=True``, so the caller can record what the
    attempt spent before deciding how to fail. Raise :class:`HarnessRunError` only when
    there is no result to report at all, such as the vendor's stream ending without one.
    """

    name: ClassVar[str]
    """Short backend identifier (e.g. ``"claude_agent_sdk"``)."""

    @abstractmethod
    def run(self, request: HarnessRequest) -> HarnessResult:
        """Run the agent once and return its result; never raises for a vendor-reported failure."""
