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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

from datetime import datetime, timezone

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
    @pytest.mark.parametrize(
        (
            "session",
            "outputs",
            "human_entries",
            "task_completed",
            "expected_conv_len",
            "expected_roles",
            "expected_content_list",
            "expected_status",
            "expected_iteration",
            "expected_current_output",
        ),
        [
            # empty_conversation
            (
                AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=1,
                    max_iterations=5,
                    prompt="Summarize",
                    current_output="Initial output",
                ),
                {1: "Initial output"},
                {},
                False,
                1,
                ["assistant"],
                ["Initial output"],
                "pending_review",
                1,
                "Initial output",
            ),
            # with_human_feedback
            (
                AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=2,
                    max_iterations=3,
                    prompt="Summarize",
                    current_output="Revised output",
                ),
                {1: "v1", 2: "Revised output"},
                {1: "Add more"},
                True,
                3,
                ["assistant", "human", "assistant"],
                ["v1", "Add more", "Revised output"],
                "pending_review",
                2,
                "Revised output",
            ),
            # skips_missing_iterations
            (
                AgentSessionData(iteration=3, max_iterations=5, prompt="", current_output="out"),
                {1: "a", 3: "out"},
                {2: "feedback"},
                False,
                3,
                ["assistant", "human", "assistant"],
                ["a", "feedback", "out"],
                "pending_review",
                3,
                "out",
            ),
            # single_iteration_no_human
            (
                AgentSessionData(iteration=1, max_iterations=2, prompt="p", current_output="o1"),
                {1: "o1"},
                {},
                False,
                1,
                ["assistant"],
                ["o1"],
                "pending_review",
                1,
                "o1",
            ),
            # multi_iteration_all_feedback
            (
                AgentSessionData(iteration=3, max_iterations=5, prompt="p", current_output="o3"),
                {1: "o1", 2: "o2", 3: "o3"},
                {1: "f1", 2: "f2"},
                False,
                5,
                ["assistant", "human", "assistant", "human", "assistant"],
                ["o1", "f1", "o2", "f2", "o3"],
                "pending_review",
                3,
                "o3",
            ),
            # sql_output
            (
                AgentSessionData(
                    iteration=1,
                    max_iterations=3,
                    prompt="Query",
                    current_output="SELECT id FROM t",
                ),
                {1: "SELECT id FROM t"},
                {},
                False,
                1,
                ["assistant"],
                ["SELECT id FROM t"],
                "pending_review",
                1,
                "SELECT id FROM t",
            ),
            # json_output (dict strified)
            (
                AgentSessionData(iteration=1, max_iterations=2, prompt="p", current_output=""),
                {1: {"result": "ok", "count": 10}},
                {},
                False,
                1,
                ["assistant"],
                [str({"result": "ok", "count": 10})],
                "pending_review",
                1,
                "",
            ),
            # json_string_output
            (
                AgentSessionData(
                    iteration=1,
                    max_iterations=3,
                    prompt="Extract",
                    current_output='{"rows": [{"id": 1}]}',
                ),
                {1: '{"rows": [{"id": 1}]}'},
                {},
                False,
                1,
                ["assistant"],
                ['{"rows": [{"id": 1}]}'],
                "pending_review",
                1,
                '{"rows": [{"id": 1}]}',
            ),
        ],
        ids=[
            "empty_conversation",
            "with_human_feedback",
            "skips_missing_iterations",
            "single_iteration_no_human",
            "multi_iteration_all_feedback",
            "sql_output",
            "json_output",
            "json_string_output",
        ],
    )
    def test_from_xcom_output_combinations(
        self,
        session,
        outputs,
        human_entries,
        task_completed,
        expected_conv_len,
        expected_roles,
        expected_content_list,
        expected_status,
        expected_iteration,
        expected_current_output,
    ):
        resp = HITLReviewResponse.from_xcom(
            dag_id="dag1",
            run_id="run1",
            task_id="task1",
            session=session,
            outputs=outputs,
            human_entries=human_entries,
            task_completed=task_completed,
        )
        assert resp.dag_id == "dag1"
        assert resp.run_id == "run1"
        assert resp.task_id == "task1"
        assert resp.status == expected_status
        assert resp.iteration == expected_iteration
        assert resp.current_output == expected_current_output
        assert resp.task_completed is task_completed
        assert len(resp.conversation) == expected_conv_len
        assert [e.role for e in resp.conversation] == expected_roles
        assert [e.content for e in resp.conversation] == expected_content_list
