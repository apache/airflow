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
"""
Shared data models, exceptions, and XCom key constants for HITL Review.

Used by both the API-server-side plugin (``plugins.hitl_review``) and the
worker-side operator mixin (``mixins.hitl_review``).  Depends only on
``pydantic`` and the standard library.

**Storage**: all session state is persisted as XCom entries on the running
task instance.  See the *XCom key constants* below for the key naming scheme.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field

HumanActionType = Literal["approve", "reject", "changes_requested"]

"""
These xcom keys are reserved for agentic operator with HITL feedback loop.
"""

_XCOM_PREFIX = "airflow_hitl_review_"

XCOM_AGENT_SESSION = f"{_XCOM_PREFIX}agent_session"
"""Session metadata written by the **worker**.

Value: ``{"status": "...", "iteration": N, "max_iterations": M,
"prompt": "...", "current_output": "..."}``.
"""

XCOM_HUMAN_ACTION = f"{_XCOM_PREFIX}human_action"
"""Human action command written by the **plugin**.

Value: ``{"action": "approve"|"reject"|"changes_requested",
"feedback": "...", "iteration": N}``.
"""

XCOM_AGENT_OUTPUT_PREFIX = f"{_XCOM_PREFIX}agent_output_"
"""Per-iteration AI output (append-only, written by worker).

Actual key: ``airflow_hitl_review_agent_output_1``, ``_2``, ...
"""

XCOM_HUMAN_FEEDBACK_PREFIX = f"{_XCOM_PREFIX}human_feedback_"
"""Per-iteration human feedback (append-only, written by plugin).

Actual key: ``airflow_hitl_review_human_feedback_1``, ``_2``, ...
"""


class SessionStatus(str, Enum):
    """Lifecycle states of a HITL review session."""

    PENDING_REVIEW = "pending_review"
    CHANGES_REQUESTED = "changes_requested"
    APPROVED = "approved"
    REJECTED = "rejected"
    MAX_ITERATIONS_EXCEEDED = "max_iterations_exceeded"
    TIMEOUT_EXCEEDED = "timeout_exceeded"


class ConversationEntry(BaseModel):
    """Single turn in the feedback conversation."""

    role: Literal["assistant", "human"]
    content: str
    iteration: int
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AgentSessionData(BaseModel):
    """
    Session metadata stored in the ``airflow_hitl_review_agent_session`` XCom.

    Written by the **worker** only.
    """

    status: SessionStatus = SessionStatus.PENDING_REVIEW
    iteration: int = 1
    max_iterations: int = 5
    prompt: str = ""
    current_output: str = ""


class HumanActionData(BaseModel):
    """
    Human action payload stored in the ``airflow_hitl_review_human_action`` XCom.

    Written by the **plugin** only. Invalid ``action`` values (e.g. typos like
    "approved") fail validation at parse time instead of causing the worker to
    loop indefinitely.
    """

    action: HumanActionType
    feedback: str = ""
    iteration: int = 1


class HumanFeedbackRequest(BaseModel):
    """Payload for the ``POST .../feedback`` endpoint."""

    feedback: str


class HITLReviewResponse(BaseModel):
    """API response for a HITL review session (combined from multiple XCom entries)."""

    dag_id: str
    run_id: str
    task_id: str
    status: SessionStatus
    iteration: int
    max_iterations: int = 5
    prompt: str
    current_output: str
    conversation: list[ConversationEntry] = []
    task_completed: bool = False

    @staticmethod
    def from_xcom(
        dag_id: str,
        run_id: str,
        task_id: str,
        session: AgentSessionData,
        outputs: dict[int, Any],
        human_entries: dict[int, Any],
        *,
        task_completed: bool = False,
    ) -> HITLReviewResponse:
        """Combine a response from separate XCom values."""
        conversation: list[ConversationEntry] = []
        for i in range(1, session.iteration + 1):
            if i in outputs:
                conversation.append(ConversationEntry(role="assistant", content=str(outputs[i]), iteration=i))
            if i in human_entries:
                conversation.append(
                    ConversationEntry(role="human", content=str(human_entries[i]), iteration=i)
                )
        return HITLReviewResponse(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            status=session.status,
            iteration=session.iteration,
            max_iterations=session.max_iterations,
            prompt=session.prompt,
            current_output=session.current_output,
            conversation=conversation,
            task_completed=task_completed,
        )
