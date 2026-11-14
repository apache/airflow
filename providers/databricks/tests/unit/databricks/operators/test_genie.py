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

import json
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowFailException, TaskDeferred
from airflow.providers.databricks.hooks.genie import DatabricksGenieError, DatabricksGenieHook
from airflow.providers.databricks.operators.genie import DatabricksGenieOperator
from airflow.providers.databricks.triggers.genie import DatabricksGenieMessageTrigger

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

SPACE_ID = "space/1"
CONVERSATION_ID = "conversation-1"
MESSAGE_ID = "message-1"
QUESTION = "How many orders were placed last week?"
START = "spaces/space%2F1/start-conversation"
FOLLOW_UP = f"spaces/space%2F1/conversations/{CONVERSATION_ID}/messages"
MESSAGE = f"spaces/space%2F1/conversations/{CONVERSATION_ID}/messages/{MESSAGE_ID}"
ATTACHMENT_RESULT = f"{MESSAGE}/attachments/attachment-1/query-result"
KEY = DatabricksGenieOperator.external_id_key
SUBMISSION_KEY = f"{KEY}_submission"

durable_only = pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)


def message(status: str, **fields) -> dict:
    return {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID, "status": status, **fields}


COMPLETED = message(
    "COMPLETED",
    attachments=[
        {"text": {"content": "There were 42 orders."}},
        {
            "attachment_id": "attachment-1",
            "query": {"title": "Orders", "query": "SELECT count(*) FROM orders"},
        },
    ],
)
QUERY_RESULT = {
    "statement_response": {
        "manifest": {"schema": {"columns": [{"name": "count"}]}, "total_row_count": 1},
        "result": {"data_array": [["42"]]},
    }
}
EXPECTED_RESULT = {
    "space_id": SPACE_ID,
    "conversation_id": CONVERSATION_ID,
    "message_id": MESSAGE_ID,
    "status": "COMPLETED",
    "answer": "There were 42 orders.",
    "queries": [{"title": "Orders", "query": "SELECT count(*) FROM orders"}],
    "query_results": [{"columns": ["count"], "rows": [["42"]], "total_row_count": 1, "truncated": False}],
}


class FakeStore:
    """Dict-backed task state store that records the order of operations."""

    def __init__(self, journal: list, data: dict | None = None) -> None:
        self.journal = journal
        self.data = dict(data or {})

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value, *, retention=None):
        self.journal.append(("set", key))
        self.data[key] = value

    def delete(self, key):
        self.journal.append(("delete", key))
        self.data.pop(key, None)


class FakeGenie:
    """Replay Genie responses per (method, endpoint) and record every request."""

    def __init__(self, journal: list) -> None:
        self.journal = journal
        self.responses: dict[tuple[str, str], list] = {}
        self.bodies: list = []

    def add(self, method: str, endpoint: str, *responses) -> None:
        self.responses.setdefault((method, endpoint), []).extend(responses)

    def __call__(self, hook, method, endpoint, *, body=None, retry_read, deadline=None):
        self.journal.append((method, endpoint))
        if method == "POST":
            assert retry_read is False
            self.bodies.append(body)
        else:
            assert retry_read is True
        queue = self.responses.get((method, endpoint))
        assert queue, f"unexpected {method} {endpoint}"
        response = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(response, BaseException):
            raise response
        return response

    def calls(self, method: str) -> list:
        return [call for call in self.journal if call[0] == method]


@pytest.fixture
def journal():
    return []


@pytest.fixture
def genie(journal):
    fake = FakeGenie(journal)
    with mock.patch.object(DatabricksGenieHook, "_request", autospec=True, side_effect=fake):
        yield fake


@pytest.fixture
def sleep():
    with mock.patch("airflow.providers.databricks.operators.genie.time.sleep", autospec=True) as sleep:
        yield sleep


def make_operator(**kwargs) -> DatabricksGenieOperator:
    kwargs.setdefault("space_id", SPACE_ID)
    kwargs.setdefault("question", QUESTION)
    kwargs.setdefault("deferrable", False)
    return DatabricksGenieOperator(task_id="ask_genie", polling_period_seconds=2, **kwargs)


def stored_ref(**overrides) -> dict:
    ref = {
        "space_id": SPACE_ID,
        "conversation_id": CONVERSATION_ID,
        "message_id": MESSAGE_ID,
        "fingerprint": make_operator()._fingerprint(),
    }
    ref.update(overrides)
    return ref


def answered(genie: FakeGenie, *statuses: str, post: str = START) -> None:
    genie.add("POST", post, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, *[message(status) for status in statuses], COMPLETED)
    genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)


@pytest.mark.parametrize(("field", "value"), [("polling_period_seconds", 0), ("timeout", -1)])
def test_invalid_wait_options(field, value):
    with pytest.raises(ValueError, match="must be positive"):
        DatabricksGenieOperator(task_id="ask", space_id=SPACE_ID, question=QUESTION, **{field: value})


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"space_id": " "}, "space ID must be a non-empty string"),
        ({"question": " "}, "question must be a non-empty string"),
        ({"question": None}, "question must be a non-empty string"),
        ({"message_id": MESSAGE_ID}, "message_id requires conversation_id"),
    ],
)
def test_invalid_request(genie, kwargs, match):
    with pytest.raises(ValueError, match=match):
        make_operator(**kwargs).execute({})
    assert genie.journal == []


def test_template_fields():
    assert set(DatabricksGenieOperator.template_fields) == {
        "space_id",
        "question",
        "databricks_conn_id",
        "conversation_id",
        "message_id",
    }


def test_hook_configuration():
    op = make_operator(databricks_conn_id="genie", databricks_retry_limit=5, databricks_retry_delay=2)
    assert op.hook.databricks_conn_id == "genie"
    assert op.hook.retry_limit == 5
    assert op.hook.retry_delay == 2
    assert op.hook.poll_interval == 2


def test_asks_new_conversation_and_waits(genie, sleep):
    answered(genie, "SUBMITTED", "EXECUTING_QUERY")

    assert make_operator(durable=False).execute({}) == EXPECTED_RESULT

    assert genie.calls("POST") == [("POST", START)]
    assert genie.bodies == [{"content": QUESTION, "enable_visualization": False}]
    assert genie.calls("GET").count(("GET", MESSAGE)) == 3
    assert sleep.call_count == 2


@pytest.mark.parametrize("empty", ["", None])
def test_empty_optional_ids_start_new_conversation(genie, empty):
    answered(genie)
    make_operator(conversation_id=empty, message_id=empty, durable=False).execute({})
    assert genie.calls("POST") == [("POST", START)]


def test_asks_follow_up_in_existing_conversation(genie):
    answered(genie, post=FOLLOW_UP)
    assert make_operator(conversation_id=CONVERSATION_ID, durable=False).execute({}) == EXPECTED_RESULT
    assert genie.calls("POST") == [("POST", FOLLOW_UP)]


def test_accepts_message_id_under_id_key(genie):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "id": MESSAGE_ID})
    genie.add("GET", MESSAGE, COMPLETED)
    genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
    assert make_operator(durable=False).execute({})["message_id"] == MESSAGE_ID


@pytest.mark.parametrize(
    ("status", "match"),
    [
        ("FAILED", "could not answer message message-1: no warehouse"),
        ("CANCELLED", "ended with status CANCELLED"),
        ("QUERY_RESULT_EXPIRED", "ended with status QUERY_RESULT_EXPIRED"),
        ("SOMETHING_NEW", "unknown message status: 'SOMETHING_NEW'"),
    ],
)
def test_remote_terminal_failure(genie, status, match):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, message(status, error={"error": "no warehouse"}))
    with pytest.raises(DatabricksGenieError, match=match):
        make_operator(durable=False).execute({})


def test_failed_message_without_detail(genie):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, message("FAILED", error="boom"))
    with pytest.raises(DatabricksGenieError, match="could not answer message message-1: boom"):
        make_operator(durable=False).execute({})


def test_synchronous_timeout(genie, sleep):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, message("ASKING_AI"))
    with (
        mock.patch(
            "airflow.providers.databricks.operators.genie.time.monotonic",
            autospec=True,
            side_effect=[0, 5, 11],
        ),
        pytest.raises(DatabricksGenieError, match="Timed out after 10 seconds .* reconnects to it"),
    ):
        make_operator(durable=False, timeout=10).execute({})
    sleep.assert_called_once_with(2)


def test_result_is_bounded(genie):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, message("COMPLETED", attachments=[{"text": {"content": "x" * 200_000}}]))

    result = make_operator(durable=False).execute({})

    assert result["truncated"] is True
    assert result["message_id"] == MESSAGE_ID
    assert len(json.dumps(result, separators=(",", ":")).encode()) <= 48 * 1024


def test_defers_with_ids_only(genie):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
    genie.add("GET", MESSAGE, message("SUBMITTED"))
    op = make_operator(durable=False, deferrable=True, databricks_conn_id="genie", timeout=60)

    with pytest.raises(TaskDeferred) as deferred:
        op.execute({})

    trigger = deferred.value.trigger
    assert isinstance(trigger, DatabricksGenieMessageTrigger)
    assert trigger.serialize()[1] == {
        "space_id": SPACE_ID,
        "conversation_id": CONVERSATION_ID,
        "message_id": MESSAGE_ID,
        "databricks_conn_id": "genie",
        "polling_period_seconds": 2,
        "retry_limit": 3,
        "retry_delay": 1,
    }
    assert deferred.value.method_name == "execute_complete"
    assert deferred.value.timeout.total_seconds() == 60
    assert deferred.value.kwargs == {"ref": stored_ref()}


def test_deferrable_does_not_defer_completed_message(genie):
    answered(genie)
    assert make_operator(durable=False, deferrable=True).execute({}) == EXPECTED_RESULT


def test_execute_complete_fetches_result_on_worker(genie):
    genie.add("GET", MESSAGE, COMPLETED)
    genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
    event = {"status": "success", "conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID}

    assert make_operator().execute_complete({}, event, ref=stored_ref()) == EXPECTED_RESULT


def test_execute_complete_raises_for_failed_message(genie):
    genie.add("GET", MESSAGE, message("CANCELLED"))
    event = {"status": "success", "conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID}
    with pytest.raises(DatabricksGenieError, match="ended with status CANCELLED"):
        make_operator().execute_complete({}, event, ref=stored_ref())


def test_execute_complete_rejects_pending_message(genie):
    genie.add("GET", MESSAGE, message("ASKING_AI"))
    event = {"status": "success", "conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID}
    with pytest.raises(DatabricksGenieError, match="has not completed yet"):
        make_operator().execute_complete({}, event, ref=stored_ref())


@pytest.mark.parametrize(("status_code", "expected"), [(503, 503), ("503", None), (None, None)])
def test_execute_complete_error_event(genie, status_code, expected):
    event = {
        "status": "error",
        "conversation_id": CONVERSATION_ID,
        "message_id": MESSAGE_ID,
        "error_type": "api_error",
        "status_code": status_code,
    }
    with pytest.raises(DatabricksGenieError, match=r"failed \(api_error\)") as err:
        make_operator().execute_complete({}, event, ref=stored_ref())
    assert err.value.status_code == expected
    assert genie.journal == []


@pytest.mark.parametrize(
    "event",
    [
        None,
        {},
        {"status": "unknown", "conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID},
        {"status": "success", "conversation_id": "other", "message_id": MESSAGE_ID},
        {"status": "success", "conversation_id": CONVERSATION_ID, "message_id": "other"},
    ],
)
def test_execute_complete_invalid_event(genie, event):
    with pytest.raises(DatabricksGenieError, match="invalid event"):
        make_operator().execute_complete({}, event, ref=stored_ref())


@pytest.mark.parametrize("status_code", [None, 500, 503])
def test_unconfirmed_submission_fails_without_retry(genie, status_code):
    genie.add("POST", START, DatabricksGenieError("unknown outcome", status_code=status_code))
    with pytest.raises(AirflowFailException, match="unknown outcome"):
        make_operator(durable=False).execute({})


def test_accepted_submission_without_ids_fails_without_retry(genie):
    genie.add("POST", START, {"conversation_id": CONVERSATION_ID})
    with pytest.raises(AirflowFailException, match="returned no conversation or message ID"):
        make_operator(durable=False).execute({})


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 429])
def test_rejected_submission_is_retryable(genie, status_code):
    genie.add("POST", START, DatabricksGenieError("rejected", status_code=status_code))
    with pytest.raises(DatabricksGenieError, match="rejected") as err:
        make_operator(durable=False).execute({})
    assert not isinstance(err.value, AirflowFailException)


def test_attach_to_existing_message_without_store(genie):
    genie.add("GET", MESSAGE, COMPLETED)
    genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
    op = make_operator(question=None, conversation_id=CONVERSATION_ID, message_id=MESSAGE_ID)
    assert op.execute({}) == EXPECTED_RESULT
    assert genie.calls("POST") == []


def test_on_kill_leaves_message_running(genie):
    make_operator().on_kill()
    assert genie.journal == []


@durable_only
class TestDurable:
    @staticmethod
    def run(op, store) -> dict:
        return op.execute({"ti": mock.MagicMock(stats_tags={}), "task_state_store": store})

    def test_persists_ids_before_waiting(self, genie, journal):
        answered(genie, "SUBMITTED")
        store = FakeStore(journal)

        assert self.run(make_operator(), store) == EXPECTED_RESULT

        first_get = journal.index(("GET", MESSAGE))
        assert journal[:first_get] == [
            ("set", SUBMISSION_KEY),
            ("POST", START),
            ("set", KEY),
            ("delete", SUBMISSION_KEY),
            ("set", KEY),
        ]
        assert store.data == {KEY: stored_ref()}

    def test_deferrable_persists_ids_before_deferring(self, genie, journal):
        genie.add("POST", START, {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID})
        genie.add("GET", MESSAGE, message("SUBMITTED"))
        store = FakeStore(journal)

        with pytest.raises(TaskDeferred):
            self.run(make_operator(deferrable=True), store)

        assert store.data == {KEY: stored_ref()}

    @pytest.mark.parametrize("status", ["SUBMITTED", "ASKING_AI", "EXECUTING_QUERY"])
    def test_reconnects_to_running_message(self, genie, journal, sleep, status):
        genie.add("GET", MESSAGE, message(status), message(status), COMPLETED)
        genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
        store = FakeStore(journal, {KEY: stored_ref()})

        assert self.run(make_operator(), store) == EXPECTED_RESULT

        assert genie.calls("POST") == []
        assert [entry for entry in journal if entry[0] in ("set", "delete")] == []

    def test_reconnect_defers_again(self, genie, journal):
        genie.add("GET", MESSAGE, message("ASKING_AI"))
        store = FakeStore(journal, {KEY: stored_ref()})

        with pytest.raises(TaskDeferred) as deferred:
            self.run(make_operator(deferrable=True), store)

        assert deferred.value.kwargs == {"ref": stored_ref()}
        assert genie.calls("POST") == []

    def test_completed_message_returns_answer_without_asking(self, genie, journal):
        genie.add("GET", MESSAGE, COMPLETED)
        genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
        store = FakeStore(journal, {KEY: stored_ref()})

        assert self.run(make_operator(), store) == EXPECTED_RESULT
        assert genie.calls("POST") == []

    @pytest.mark.parametrize(
        ("stored", "first_response"),
        [
            pytest.param(stored_ref(), message("FAILED"), id="failed"),
            pytest.param(stored_ref(), message("CANCELLED"), id="cancelled"),
            pytest.param(stored_ref(), DatabricksGenieError("gone", status_code=404), id="deleted"),
            pytest.param(stored_ref(fingerprint="other"), None, id="other-question"),
            pytest.param("legacy-id", None, id="unexpected-shape"),
        ],
    )
    def test_asks_again_when_stored_message_cannot_be_used(self, genie, journal, stored, first_response):
        if first_response is not None:
            genie.add("GET", MESSAGE, first_response)
        answered(genie)
        store = FakeStore(journal, {KEY: stored})

        assert self.run(make_operator(), store) == EXPECTED_RESULT

        assert genie.calls("POST") == [("POST", START)]
        assert store.data == {KEY: stored_ref()}

    def test_stored_message_lookup_error_propagates(self, genie, journal):
        genie.add("GET", MESSAGE, DatabricksGenieError("denied", status_code=403))
        store = FakeStore(journal, {KEY: stored_ref()})

        with pytest.raises(DatabricksGenieError, match="denied"):
            self.run(make_operator(), store)
        assert genie.calls("POST") == []

    def test_changed_question_does_not_reuse_answer(self, genie, journal):
        genie.add("POST", START, DatabricksGenieError("rejected", status_code=400))
        store = FakeStore(journal, {KEY: stored_ref()})

        with pytest.raises(DatabricksGenieError, match="rejected"):
            self.run(make_operator(question="Something else?"), store)

        assert genie.calls("GET") == []
        assert genie.bodies == [{"content": "Something else?", "enable_visualization": False}]

    def test_unconfirmed_submission_blocks_next_try(self, genie, journal):
        genie.add("POST", START, DatabricksGenieError("unknown outcome", status_code=None))
        store = FakeStore(journal)

        with pytest.raises(AirflowFailException, match="unknown outcome"):
            self.run(make_operator(), store)
        assert store.data == {SUBMISSION_KEY: {"fingerprint": make_operator()._fingerprint()}}

        with pytest.raises(AirflowFailException, match="never confirmed the outcome"):
            self.run(make_operator(), store)
        assert genie.calls("POST") == [("POST", START)]

    def test_accepted_submission_without_ids_blocks_next_try(self, genie, journal):
        genie.add("POST", START, {"message_id": MESSAGE_ID})
        store = FakeStore(journal)

        with pytest.raises(AirflowFailException, match="returned no conversation or message ID"):
            self.run(make_operator(), store)
        assert SUBMISSION_KEY in store.data

    def test_resubmit_unconfirmed_asks_again(self, genie, journal):
        answered(genie)
        store = FakeStore(journal, {SUBMISSION_KEY: {"fingerprint": "x"}})

        assert self.run(make_operator(resubmit_unconfirmed=True), store) == EXPECTED_RESULT
        assert store.data == {KEY: stored_ref()}

    @pytest.mark.parametrize("status_code", [400, 403, 429])
    def test_rejected_submission_clears_marker(self, genie, journal, status_code):
        genie.add("POST", START, DatabricksGenieError("rejected", status_code=status_code))
        store = FakeStore(journal)

        with pytest.raises(DatabricksGenieError, match="rejected"):
            self.run(make_operator(), store)
        assert store.data == {}

    def test_attach_recovers_unconfirmed_submission(self, genie, journal):
        genie.add("GET", MESSAGE, COMPLETED)
        genie.add("GET", ATTACHMENT_RESULT, QUERY_RESULT)
        other = stored_ref(message_id="older")
        store = FakeStore(journal, {SUBMISSION_KEY: {"fingerprint": "x"}, KEY: other})
        op = make_operator(conversation_id=CONVERSATION_ID, message_id=MESSAGE_ID)

        assert self.run(op, store) == EXPECTED_RESULT

        assert genie.calls("POST") == []
        assert store.data == {KEY: {**stored_ref(), "fingerprint": op._fingerprint()}}

    def test_not_durable_ignores_store(self, genie, journal):
        answered(genie)
        store = FakeStore(journal, {KEY: stored_ref(), SUBMISSION_KEY: {"fingerprint": "x"}})

        assert self.run(make_operator(durable=False), store) == EXPECTED_RESULT

        assert genie.calls("POST") == [("POST", START)]
        assert [entry for entry in journal if entry[0] in ("set", "delete")] == []

    def test_cached_result_is_not_fetched_twice(self, genie, journal):
        answered(genie)
        self.run(make_operator(), FakeStore(journal))
        assert genie.calls("GET").count(("GET", ATTACHMENT_RESULT)) == 1
