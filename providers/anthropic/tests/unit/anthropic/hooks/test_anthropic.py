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

from airflow.providers.anthropic.exceptions import (
    AnthropicAgentSessionError,
    AnthropicAgentSessionTimeout,
    AnthropicBatchTimeout,
    AnthropicError,
    AnthropicTriggerEventError,
)
from airflow.providers.anthropic.hooks.anthropic import (
    DEFAULT_MODEL,
    MAX_CONSECUTIVE_POLL_FAILURES,
    AnthropicHook,
    BatchStatus,
    SessionStatus,
    evaluate_session_state,
    validate_execute_complete_event,
)

pytest.importorskip("anthropic")

HOOK_PATH = "airflow.providers.anthropic.hooks.anthropic"


def _conn(password="sk-ant-test", host=None, extra=None):
    conn = mock.MagicMock()
    conn.password = password
    conn.host = host
    conn.extra_dejson = extra or {}
    return conn


def _make_hook(extra=None):
    """Build a hook with the cached connection + client pre-injected (no real lookup)."""
    hook = AnthropicHook()
    hook.__dict__["_connection"] = _conn(extra=extra)
    hook.__dict__["platform"] = (extra or {}).get("platform", "anthropic").lower()
    client = mock.MagicMock()
    hook.__dict__["conn"] = client
    return hook, client


class TestBatchStatus:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [("in_progress", True), ("canceling", True), ("ended", False)],
    )
    def test_is_in_progress(self, status, expected):
        assert BatchStatus.is_in_progress(status) is expected


class TestSessionStatus:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [("running", False), ("rescheduling", False), ("idle", True), ("terminated", True)],
    )
    def test_is_terminal(self, status, expected):
        assert SessionStatus.is_terminal(status) is expected


class TestValidateTriggerEvent:
    @pytest.mark.parametrize(
        ("event", "match"),
        [
            pytest.param(None, "event is None", id="none"),
            pytest.param({}, "Unexpected trigger event status None", id="missing-status"),
            pytest.param(
                {"status": "ended", "batch_id": "b"}, "Unexpected trigger event status", id="unknown-status"
            ),
        ],
    )
    def test_invalid_event_raises(self, event, match):
        with pytest.raises(AnthropicTriggerEventError, match=match):
            validate_execute_complete_event(event)

    @pytest.mark.parametrize(
        "event",
        [
            pytest.param({"status": "success", "batch_id": "b"}, id="success"),
            pytest.param({"status": "error", "batch_id": "b", "message": "boom"}, id="error"),
            pytest.param({"status": "timeout", "batch_id": "b", "message": "slow"}, id="timeout"),
        ],
    )
    def test_valid_event_is_returned(self, event):
        assert validate_execute_complete_event(event) is event


def _session(status, outcome_results=None):
    s = mock.MagicMock()
    s.status = status
    s.id = "sess_1"
    s.outcome_evaluations = [mock.MagicMock(result=r) for r in (outcome_results or [])]
    return s


def _idle_event(reason, event_id="evt_idle"):
    return mock.MagicMock(type="session.status_idle", id=event_id, stop_reason=mock.MagicMock(type=reason))


class TestEvaluateSessionState:
    def test_terminated_is_done_with_error(self):
        done, err, check = evaluate_session_state(_session("terminated"), expect_outcome=False)
        assert done is True
        assert err is not None
        assert "terminated" in err
        assert check is False

    @pytest.mark.parametrize("status", ["running", "rescheduling"])
    def test_non_idle_not_done(self, status):
        assert evaluate_session_state(_session(status), expect_outcome=False) == (False, None, False)

    def test_message_idle_needs_event_check(self):
        # message run: the session object can't say why it's idle -> defer to the event log
        assert evaluate_session_state(_session("idle"), expect_outcome=False) == (False, None, True)

    def test_outcome_idle_without_verdict_not_done(self):
        # just-created idle: no verdict yet -> keep waiting (fixes the start race)
        assert evaluate_session_state(_session("idle"), expect_outcome=True) == (False, None, False)

    def test_outcome_satisfied_is_done_success(self):
        assert evaluate_session_state(_session("idle", ["satisfied"]), expect_outcome=True) == (
            True,
            None,
            False,
        )

    @pytest.mark.parametrize("result", ["failed", "max_iterations_reached", "interrupted"])
    def test_outcome_failure_is_done_with_error(self, result):
        done, err, check = evaluate_session_state(_session("idle", [result]), expect_outcome=True)
        assert done is True
        assert err is not None
        assert result in err


class TestPollSessionCompletion:
    def test_terminated_is_error(self):
        hook, client = _make_hook()
        client.beta.sessions.retrieve.return_value = _session("terminated")
        done, err = hook.poll_session_completion("s")
        assert done is True
        assert err is not None

    def test_message_end_turn_success(self):
        hook, client = _make_hook()
        client.beta.sessions.retrieve.return_value = _session("idle")
        client.beta.sessions.events.list.return_value = [_idle_event("end_turn")]
        assert hook.poll_session_completion("s", kickoff_event_id="evt_kick") == (True, None)

    @pytest.mark.parametrize("reason", ["requires_action", "retries_exhausted"])
    def test_message_blocked_is_error(self, reason):
        hook, client = _make_hook()
        client.beta.sessions.retrieve.return_value = _session("idle")
        client.beta.sessions.events.list.return_value = [_idle_event(reason)]
        done, err = hook.poll_session_completion("s", kickoff_event_id="evt_kick")
        assert done is True
        assert err is not None
        assert reason in err

    def test_message_no_response_yet_not_done(self):
        # newest event is our kickoff (agent hasn't responded) -> keep waiting (start race)
        hook, client = _make_hook()
        client.beta.sessions.retrieve.return_value = _session("idle")
        client.beta.sessions.events.list.return_value = [mock.MagicMock(type="user.message", id="evt_kick")]
        assert hook.poll_session_completion("s", kickoff_event_id="evt_kick") == (False, None)

    def test_outcome_satisfied_skips_event_check(self):
        hook, client = _make_hook()
        client.beta.sessions.retrieve.return_value = _session("idle", ["satisfied"])
        assert hook.poll_session_completion("s", expect_outcome=True) == (True, None)
        client.beta.sessions.events.list.assert_not_called()


class TestDefaultModel:
    def test_defaults_to_constant(self):
        hook, _ = _make_hook()
        assert hook.default_model == DEFAULT_MODEL

    def test_from_connection_extra(self):
        hook, _ = _make_hook(extra={"model": "claude-sonnet-4-6"})
        assert hook.default_model == "claude-sonnet-4-6"

    def test_create_message_uses_connection_model(self):
        hook, client = _make_hook(extra={"model": "claude-sonnet-4-6"})
        hook.create_message([{"role": "user", "content": "hi"}])
        assert client.messages.create.call_args.kwargs["model"] == "claude-sonnet-4-6"

    def test_explicit_model_overrides_connection(self):
        hook, client = _make_hook(extra={"model": "claude-sonnet-4-6"})
        hook.create_message([{"role": "user", "content": "hi"}], model="claude-haiku-4-5")
        assert client.messages.create.call_args.kwargs["model"] == "claude-haiku-4-5"

    def test_create_agent_uses_connection_model(self):
        hook, client = _make_hook(extra={"model": "claude-sonnet-4-6"})
        hook.create_agent(name="a")
        assert client.beta.agents.create.call_args.kwargs["model"] == "claude-sonnet-4-6"

    @pytest.mark.parametrize(
        "model",
        [None, "my-anthropic.model"],  # bare default id and a substring that is not a real prefix
    )
    def test_bedrock_rejects_invalid_model_id(self, model):
        extra = {"platform": "bedrock", **({"model": model} if model else {})}
        hook, client = _make_hook(extra=extra)
        with pytest.raises(AnthropicError, match="not a valid Amazon Bedrock model id"):
            hook.create_message([{"role": "user", "content": "hi"}])
        client.messages.create.assert_not_called()

    @pytest.mark.parametrize(
        ("platform", "model", "expected"),
        [
            (
                "bedrock",
                "anthropic.claude-sonnet-4-5-20250929-v1:0",
                "anthropic.claude-sonnet-4-5-20250929-v1:0",
            ),
            ("bedrock", "global.anthropic.claude-opus-4-6-v1", "global.anthropic.claude-opus-4-6-v1"),
            (
                "bedrock",
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
            ),
            ("vertex", None, DEFAULT_MODEL),  # non-Bedrock platforms skip the guard
        ],
    )
    def test_accepts_valid_model_id(self, platform, model, expected):
        extra = {"platform": platform, **({"model": model} if model else {})}
        hook, client = _make_hook(extra=extra)
        hook.create_message([{"role": "user", "content": "hi"}])
        assert client.messages.create.call_args.kwargs["model"] == expected


class TestManagedAgentsHook:
    def _hook_with_client(self, extra=None):
        hook = AnthropicHook()
        hook.__dict__["_connection"] = _conn(extra=extra)
        hook.__dict__["platform"] = (extra or {}).get("platform", "anthropic").lower()
        client = mock.MagicMock()
        hook.__dict__["conn"] = client
        return hook, client

    def test_create_agent(self):
        hook, client = self._hook_with_client()
        hook.create_agent(name="ag1", system="be brief")
        client.beta.agents.create.assert_called_once_with(name="ag1", model=DEFAULT_MODEL, system="be brief")

    def test_archive_session(self):
        hook, client = self._hook_with_client()
        hook.archive_session("sess_1")
        client.beta.sessions.archive.assert_called_once_with("sess_1")

    def test_create_session(self):
        hook, client = self._hook_with_client()
        hook.create_session(agent="ag", environment_id="env")
        client.beta.sessions.create.assert_called_once_with(agent="ag", environment_id="env")

    def test_create_environment_defaults_cloud(self):
        hook, client = self._hook_with_client()
        hook.create_environment(name="env1")
        client.beta.environments.create.assert_called_once_with(
            name="env1", config={"type": "cloud", "networking": {"type": "unrestricted"}}
        )

    def test_send_event_wraps_in_events_list(self):
        hook, client = self._hook_with_client()
        event = {"type": "user.message", "content": [{"type": "text", "text": "hi"}]}
        hook.send_event("sess_1", event)
        client.beta.sessions.events.send.assert_called_once_with("sess_1", events=[event])

    def test_get_session(self):
        hook, client = self._hook_with_client()
        hook.get_session("sess_1")
        client.beta.sessions.retrieve.assert_called_once_with("sess_1")

    @pytest.mark.parametrize("platform", ["bedrock", "vertex", "foundry"])
    def test_managed_agents_unavailable_on_non_first_party(self, platform):
        hook, _ = self._hook_with_client(extra={"platform": platform})
        with pytest.raises(AnthropicError, match="Managed Agents is not available"):
            hook.create_session(agent="ag", environment_id="env")


class TestWaitForSession:
    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "poll_session_completion")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_returns_when_done(self, mock_get_connection, mock_poll, mock_sleep):
        mock_get_connection.return_value = _conn()
        mock_poll.side_effect = [(False, None), (True, None)]
        AnthropicHook().wait_for_session("sess_1", poll_interval=0.01)
        assert mock_poll.call_count == 2

    @mock.patch(f"{HOOK_PATH}.time.monotonic")
    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "poll_session_completion")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_raises_on_timeout(self, mock_get_connection, mock_poll, mock_sleep, mock_monotonic):
        mock_get_connection.return_value = _conn()
        mock_poll.return_value = (False, None)
        mock_monotonic.side_effect = [0, 100]
        with pytest.raises(AnthropicAgentSessionTimeout, match="did not reach a terminal status"):
            AnthropicHook().wait_for_session("sess_1", poll_interval=0.01, timeout=10)

    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "poll_session_completion")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_failure_raises(self, mock_get_connection, mock_poll, mock_sleep):
        mock_get_connection.return_value = _conn()
        mock_poll.return_value = (True, "Outcome not satisfied for session sess_1: failed.")
        with pytest.raises(AnthropicAgentSessionError, match="not satisfied"):
            AnthropicHook().wait_for_session("sess_1", expect_outcome=True, poll_interval=0.01)


class TestAnthropicHookGetConn:
    @mock.patch(f"{HOOK_PATH}.Anthropic")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_first_party_uses_password_and_host(self, mock_get_connection, mock_anthropic):
        mock_get_connection.return_value = _conn(password="sk-ant", host="https://gw.example")
        client = AnthropicHook().get_conn()
        mock_anthropic.assert_called_once_with(api_key="sk-ant", base_url="https://gw.example")
        assert client is mock_anthropic.return_value

    @mock.patch(f"{HOOK_PATH}.Anthropic")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_client_kwargs_override_api_key_and_base_url(self, mock_get_connection, mock_anthropic):
        mock_get_connection.return_value = _conn(
            password="from-password",
            extra={"anthropic_client_kwargs": {"api_key": "from-extra", "max_retries": 5}},
        )
        AnthropicHook().get_conn()
        mock_anthropic.assert_called_once_with(api_key="from-extra", base_url=None, max_retries=5)

    @mock.patch(f"{HOOK_PATH}.AnthropicBedrock")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_bedrock_platform(self, mock_get_connection, mock_bedrock):
        mock_get_connection.return_value = _conn(extra={"platform": "bedrock", "aws_region": "us-east-1"})
        client = AnthropicHook().get_conn()
        mock_bedrock.assert_called_once_with(aws_region="us-east-1")
        assert client is mock_bedrock.return_value

    @mock.patch(f"{HOOK_PATH}.AnthropicVertex")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_vertex_platform(self, mock_get_connection, mock_vertex):
        mock_get_connection.return_value = _conn(
            extra={"platform": "vertex", "project_id": "p1", "region": "us-central1"}
        )
        AnthropicHook().get_conn()
        mock_vertex.assert_called_once_with(project_id="p1", region="us-central1")

    @mock.patch(f"{HOOK_PATH}.AnthropicAWS", autospec=True)
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_aws_platform(self, mock_get_connection, mock_aws):
        mock_get_connection.return_value = _conn(extra={"platform": "aws", "aws_region": "us-east-1"})
        AnthropicHook().get_conn()
        mock_aws.assert_called_once_with(aws_region="us-east-1")

    @mock.patch(f"{HOOK_PATH}.AnthropicAWS", autospec=True)
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_aws_platform_without_region(self, mock_get_connection, mock_aws):
        # No aws_region configured -> pass None so the SDK falls back to its own resolution.
        mock_get_connection.return_value = _conn(extra={"platform": "aws"})
        AnthropicHook().get_conn()
        mock_aws.assert_called_once_with(aws_region=None)

    @mock.patch(f"{HOOK_PATH}.AnthropicFoundry")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_foundry_platform(self, mock_get_connection, mock_foundry):
        mock_get_connection.return_value = _conn(
            password="azkey", extra={"platform": "foundry", "resource": "r1"}
        )
        AnthropicHook().get_conn()
        mock_foundry.assert_called_once_with(api_key="azkey", resource="r1")

    @mock.patch.object(AnthropicHook, "get_connection")
    def test_unknown_platform_raises(self, mock_get_connection):
        mock_get_connection.return_value = _conn(extra={"platform": "bogus"})
        with pytest.raises(AnthropicError, match="Unknown Anthropic platform"):
            AnthropicHook().get_conn()

    @mock.patch(f"{HOOK_PATH}.Anthropic")
    @mock.patch(f"{HOOK_PATH}.IdentityTokenFile")
    @mock.patch(f"{HOOK_PATH}.WorkloadIdentityCredentials")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_workload_identity_federation_explicit(
        self, mock_get_connection, mock_wic, mock_itf, mock_anthropic
    ):
        mock_get_connection.return_value = _conn(
            password=None,
            extra={
                "workload_identity": {
                    "identity_token_file": "/var/run/secrets/anthropic.com/token",
                    "federation_rule_id": "fdrl_x",
                    "organization_id": "org_x",
                    "service_account_id": "svac_x",
                    "workspace_id": "wrkspc_x",
                }
            },
        )
        AnthropicHook().get_conn()
        mock_itf.assert_called_once_with("/var/run/secrets/anthropic.com/token")
        mock_wic.assert_called_once_with(
            identity_token_provider=mock_itf.return_value,
            federation_rule_id="fdrl_x",
            organization_id="org_x",
            service_account_id="svac_x",
            workspace_id="wrkspc_x",
        )
        mock_anthropic.assert_called_once_with(credentials=mock_wic.return_value, base_url=None)

    @mock.patch(f"{HOOK_PATH}.Anthropic")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_no_key_lets_sdk_resolve_credentials(self, mock_get_connection, mock_anthropic):
        # No static key and no explicit WIF config: construct without api_key so the SDK
        # resolves env-driven Workload Identity Federation / profile credentials.
        mock_get_connection.return_value = _conn(password=None)
        AnthropicHook().get_conn()
        mock_anthropic.assert_called_once_with(base_url=None)


class TestAnthropicHookFeatures:
    def _hook_with_client(self, extra=None):
        hook = AnthropicHook()
        # Pre-populate the cached_property values so no real connection lookup or
        # client construction happens.
        hook.__dict__["_connection"] = _conn(extra=extra)
        hook.__dict__["platform"] = (extra or {}).get("platform", "anthropic").lower()
        client = mock.MagicMock()
        hook.__dict__["conn"] = client
        return hook, client

    def test_create_message_passes_params(self):
        hook, client = self._hook_with_client()
        hook.create_message([{"role": "user", "content": "hi"}], system="be brief")
        client.messages.create.assert_called_once_with(
            model=DEFAULT_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": "hi"}],
            system="be brief",
        )

    def test_count_tokens_returns_input_tokens(self):
        hook, client = self._hook_with_client()
        client.messages.count_tokens.return_value.input_tokens = 42
        assert hook.count_tokens([{"role": "user", "content": "hi"}]) == 42

    def test_create_batch_passes_requests(self):
        hook, client = self._hook_with_client()
        reqs = [{"custom_id": "a", "params": {"model": DEFAULT_MODEL, "max_tokens": 1, "messages": []}}]
        hook.create_batch(reqs)
        client.messages.batches.create.assert_called_once_with(requests=reqs)

    def test_create_batch_defaults_missing_model_from_conn(self):
        hook, client = self._hook_with_client(extra={"model": "claude-sonnet-4-6"})
        hook.create_batch([{"custom_id": "a", "params": {"max_tokens": 1, "messages": []}}])
        sent = client.messages.batches.create.call_args.kwargs["requests"]
        assert sent[0]["params"]["model"] == "claude-sonnet-4-6"

    def test_create_batch_preserves_explicit_request_model(self):
        hook, client = self._hook_with_client(extra={"model": "claude-sonnet-4-6"})
        hook.create_batch(
            [{"custom_id": "a", "params": {"model": "claude-haiku-4-5", "max_tokens": 1, "messages": []}}]
        )
        sent = client.messages.batches.create.call_args.kwargs["requests"]
        assert sent[0]["params"]["model"] == "claude-haiku-4-5"

    def test_create_batch_model_arg_overrides_conn_default(self):
        hook, client = self._hook_with_client(extra={"model": "claude-sonnet-4-6"})
        hook.create_batch(
            [{"custom_id": "a", "params": {"max_tokens": 1, "messages": []}}], model="claude-opus-4-8"
        )
        sent = client.messages.batches.create.call_args.kwargs["requests"]
        assert sent[0]["params"]["model"] == "claude-opus-4-8"

    def test_create_batch_does_not_mutate_caller_requests(self):
        hook, _ = self._hook_with_client(extra={"model": "claude-sonnet-4-6"})
        original = [{"custom_id": "a", "params": {"max_tokens": 1, "messages": []}}]
        hook.create_batch(original)
        assert "model" not in original[0]["params"]

    def test_get_and_cancel_batch(self):
        hook, client = self._hook_with_client()
        hook.get_batch("batch_1")
        client.messages.batches.retrieve.assert_called_once_with("batch_1")
        hook.cancel_batch("batch_1")
        client.messages.batches.cancel.assert_called_once_with("batch_1")

    def test_stream_batch_results_is_generator(self):
        hook, client = self._hook_with_client()
        client.messages.batches.results.return_value = iter([1, 2, 3])
        assert list(hook.stream_batch_results("batch_1")) == [1, 2, 3]

    @pytest.mark.parametrize("platform", ["bedrock", "vertex", "foundry"])
    def test_batch_features_unavailable_on_non_first_party(self, platform):
        hook, _ = self._hook_with_client(extra={"platform": platform})
        with pytest.raises(AnthropicError, match="not available on the"):
            hook.create_batch([])
        with pytest.raises(AnthropicError, match="not available on the"):
            hook.count_tokens([{"role": "user", "content": "x"}])


class TestWaitForBatch:
    def _batch(self, status):
        batch = mock.MagicMock()
        batch.processing_status = status
        return batch

    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "get_batch")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_returns_when_ended(self, mock_get_connection, mock_get_batch, mock_sleep):
        mock_get_connection.return_value = _conn()
        mock_get_batch.side_effect = [self._batch("in_progress"), self._batch("ended")]
        AnthropicHook().wait_for_batch("batch_1", wait_seconds=0.01)
        assert mock_get_batch.call_count == 2

    @mock.patch(f"{HOOK_PATH}.time.monotonic")
    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "get_batch")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_raises_on_timeout(self, mock_get_connection, mock_get_batch, mock_sleep, mock_monotonic):
        mock_get_connection.return_value = _conn()
        mock_get_batch.return_value = self._batch("in_progress")
        mock_monotonic.side_effect = [0, 100]  # start=0, elapsed=100 > timeout
        with pytest.raises(AnthropicBatchTimeout, match="did not reach a terminal status"):
            AnthropicHook().wait_for_batch("batch_1", wait_seconds=0.01, timeout=10)

    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "get_batch")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_tolerates_transient_poll_error(self, mock_get_connection, mock_get_batch, mock_sleep):
        mock_get_connection.return_value = _conn()
        mock_get_batch.side_effect = [RuntimeError("blip"), self._batch("ended")]
        AnthropicHook().wait_for_batch("batch_1", wait_seconds=0.01)
        assert mock_get_batch.call_count == 2

    @mock.patch(f"{HOOK_PATH}.time.sleep")
    @mock.patch.object(AnthropicHook, "get_batch")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_gives_up_after_consecutive_poll_failures(self, mock_get_connection, mock_get_batch, mock_sleep):
        mock_get_connection.return_value = _conn()
        mock_get_batch.side_effect = RuntimeError("persistent")
        with pytest.raises(RuntimeError, match="persistent"):
            AnthropicHook().wait_for_batch("batch_1", wait_seconds=0.01)
        assert mock_get_batch.call_count == MAX_CONSECUTIVE_POLL_FAILURES


class TestTestConnection:
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_first_party_calls_models_list(self, mock_get_connection):
        mock_get_connection.return_value = _conn()
        hook = AnthropicHook()
        client = mock.MagicMock()
        hook.__dict__["conn"] = client
        ok, msg = hook.test_connection()
        assert ok is True
        client.models.list.assert_called_once()

    @mock.patch.object(AnthropicHook, "get_conn")
    @mock.patch.object(AnthropicHook, "get_connection")
    def test_non_first_party_skips_live_check(self, mock_get_connection, mock_get_conn):
        mock_get_connection.return_value = _conn(extra={"platform": "bedrock", "aws_region": "us-east-1"})
        ok, msg = AnthropicHook().test_connection()
        assert ok is True
        assert "no live check" in msg
        mock_get_conn.assert_called_once()
