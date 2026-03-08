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

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
    AgentSessionData,
    ConversationEntry,
    HITLReviewResponse,
    HumanActionData,
    HumanFeedbackRequest,
    SessionStatus,
)


class TestXComConstants:
    def test_xcom_keys_have_expected_prefix(self):
        assert XCOM_AGENT_SESSION.startswith("airflow_hitl_review_")
        assert XCOM_HUMAN_ACTION.startswith("airflow_hitl_review_")
        assert XCOM_AGENT_OUTPUT_PREFIX.startswith("airflow_hitl_review_")
        assert XCOM_HUMAN_FEEDBACK_PREFIX.startswith("airflow_hitl_review_")

    def test_xcom_agent_output_prefix_ends_with_underscore(self):
        assert XCOM_AGENT_OUTPUT_PREFIX.endswith("_")


class TestConversationEntry:
    def test_from_fields(self):
        entry = ConversationEntry(role="assistant", content="Hello", iteration=1)
        assert entry.role == "assistant"
        assert entry.content == "Hello"
        assert entry.iteration == 1
        assert isinstance(entry.timestamp, datetime)

    def test_timestamp_defaults_to_utc_now(self):
        before = datetime.now(timezone.utc)
        entry = ConversationEntry(role="human", content="Hi", iteration=2)
        after = datetime.now(timezone.utc)
        assert before <= entry.timestamp <= after


class TestAgentSessionData:
    def test_defaults(self):
        session = AgentSessionData()
        assert session.status == SessionStatus.PENDING_REVIEW
        assert session.iteration == 1
        assert session.max_iterations == 5
        assert session.prompt == ""
        assert session.current_output == ""

    def test_from_dict(self):
        session = AgentSessionData(
            status=SessionStatus.CHANGES_REQUESTED,
            iteration=2,
            max_iterations=3,
            prompt="Summarize",
            current_output="Draft text",
        )
        assert session.status == SessionStatus.CHANGES_REQUESTED
        assert session.iteration == 2
        assert session.max_iterations == 3
        assert session.prompt == "Summarize"
        assert session.current_output == "Draft text"


class TestHumanActionData:
    def test_defaults(self):
        action = HumanActionData(action="approve")
        assert action.action == "approve"
        assert action.feedback == ""
        assert action.iteration == 1

    def test_full(self):
        action = HumanActionData(
            action="changes_requested",
            feedback="Add more detail",
            iteration=2,
        )
        assert action.action == "changes_requested"
        assert action.feedback == "Add more detail"
        assert action.iteration == 2

    def test_invalid_action_rejects_at_parse_time(self):
        """Invalid action (e.g. typo 'approved') fails validation to avoid worker looping forever."""
        with pytest.raises(ValidationError, match="action"):
            HumanActionData(action="approved")
        with pytest.raises(ValidationError, match="action"):
            HumanActionData(action="rejected")
        with pytest.raises(ValidationError, match="action"):
            HumanActionData(action="unknown")


class TestHumanFeedbackRequest:
    def test_feedback_field(self):
        req = HumanFeedbackRequest(feedback="Please expand section 2")
        assert req.feedback == "Please expand section 2"


class TestHITLReviewResponse:
    def test_from_xcom_empty_conversation(self):
        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=5,
            prompt="Summarize",
            current_output="Initial output",
        )
        resp = HITLReviewResponse.from_xcom(
            dag_id="dag1",
            run_id="run1",
            task_id="task1",
            session=session,
            outputs={1: "Initial output"},
            human_entries={},
        )
        assert resp.dag_id == "dag1"
        assert resp.run_id == "run1"
        assert resp.task_id == "task1"
        assert resp.status == "pending_review"
        assert resp.iteration == 1
        assert resp.max_iterations == 5
        assert resp.prompt == "Summarize"
        assert resp.current_output == "Initial output"
        assert len(resp.conversation) == 1
        entry = resp.conversation[0]
        assert entry.role == "assistant"
        assert entry.content == "Initial output"
        assert entry.iteration == 1
        assert resp.task_completed is False

    def test_from_xcom_with_human_feedback(self):
        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=2,
            max_iterations=3,
            prompt="Summarize",
            current_output="Revised output",
        )
        resp = HITLReviewResponse.from_xcom(
            dag_id="d",
            run_id="r",
            task_id="t",
            session=session,
            outputs={1: "v1", 2: "Revised output"},
            human_entries={1: "Add more"},
            task_completed=True,
        )
        assert resp.iteration == 2
        assert resp.max_iterations == 3
        assert resp.task_completed is True
        assert len(resp.conversation) == 3  # assistant(1), human(1), assistant(2)
        roles = [e.role for e in resp.conversation]
        assert roles == ["assistant", "human", "assistant"]
        contents = [e.content for e in resp.conversation]
        assert "v1" in contents
        assert "Add more" in contents
        assert "Revised output" in contents

    def test_from_xcom_skips_missing_iterations(self):
        session = AgentSessionData(
            iteration=3, max_iterations=5, prompt="", current_output="out"
        )
        resp = HITLReviewResponse.from_xcom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            session=session,
            outputs={1: "a", 3: "out"},
            human_entries={2: "feedback"},
        )
        assert len(resp.conversation) == 3  # assistant(1), human(2), assistant(3)
        assert any(e.content == "feedback" and e.iteration == 2 for e in resp.conversation)
