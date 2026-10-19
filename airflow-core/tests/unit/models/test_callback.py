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
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import structlog
from sqlalchemy import select

from airflow._shared.module_loading import accepts_context
from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.executors.workloads.callback import CALLBACK_KWARGS_ENCODING_KEY, CallbackDTO
from airflow.models import Trigger
from airflow.models.callback import (
    Callback,
    CallbackFetchMethod,
    CallbackState,
    DagProcessorCallback,
    ExecutorCallback,
    TriggererCallback,
)
from airflow.sdk import serde
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.sdk.execution_time.callback_supervisor import execute_callback
from airflow.serialization.decoders import decode_deadline_alert, smart_decode_trigger_kwargs
from airflow.serialization.encoders import encode_deadline_alert
from airflow.triggers.base import TriggerEvent
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, CallbackTrigger
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_callbacks

pytestmark = [pytest.mark.db_test]


async def async_callback():
    """Empty awaitable callable used for unit tests."""
    pass


def sync_callback():
    """Empty (sync) callable used for unit tests"""
    pass


TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_ASYNC_CALLBACK = AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS)
TEST_SYNC_CALLBACK = SyncCallback(sync_callback, kwargs=TEST_CALLBACK_KWARGS)
TEST_DAG_ID = "test_dag"


class _TrackedKwarg:
    """A class named in callback kwargs, to tell where the kwargs are decoded."""

    instantiated = False

    def __init__(self):
        type(self).instantiated = True

    def serialize(self):
        return {}

    @staticmethod
    def deserialize(data, version):
        return _TrackedKwarg()


_TRACKED_KWARG_NAME = f"{_TrackedKwarg.__module__}.{_TrackedKwarg.__qualname__}"

ENCODED_CALLBACK_KWARGS = {
    "current": {
        "interval": {serde.CLASSNAME: "datetime.timedelta", serde.VERSION: 2, serde.DATA: 3600.0},
        "pair": {serde.CLASSNAME: "builtins.tuple", serde.VERSION: 1, serde.DATA: [1, 2]},
        "plain": {"nested": [1, "two"]},
        "tracked": {serde.CLASSNAME: _TRACKED_KWARG_NAME, serde.VERSION: 0, serde.DATA: {}},
    },
    "legacy": {
        "interval": {"__type": "timedelta", "__var": 3600.0},
        "pair": {"__type": "tuple", "__var": [1, 2]},
        "plain": {"nested": [1, "two"]},
        "tracked": {"__type": _TRACKED_KWARG_NAME, "__var": {}},
    },
}

received_sync_kwargs: dict = {}


def recording_sync_callback(**kwargs):
    """Sync callable which records the kwargs it was called with."""
    received_sync_kwargs.clear()
    received_sync_kwargs.update(kwargs)


def _decode_callback_def(callback_def, encoded_kwargs):
    """Return ``callback_def`` as the scheduler sees it after decoding the stored deadline alert."""
    encoded = encode_deadline_alert(
        DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT, interval=timedelta(hours=1), callback=callback_def
        )
    )
    encoded["callback"][serde.DATA]["kwargs"] = encoded_kwargs
    return decode_deadline_alert(encoded).callback


def _assert_decoded(kwargs):
    assert isinstance(kwargs.pop("tracked"), _TrackedKwarg)
    assert kwargs == {"interval": timedelta(hours=1), "pair": (1, 2), "plain": {"nested": [1, "two"]}}


@pytest.fixture
def allow_tracked_kwarg():
    _TrackedKwarg.instantiated = False
    with patch.object(serde, "_extra_allowed", serde._extra_allowed | {_TRACKED_KWARG_NAME}):
        yield


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.fixture(scope="module", autouse=True)
def clean_db(request):
    yield
    clear_db_callbacks()


class TestCallback:
    @pytest.mark.parametrize(
        ("callback_def", "expected_cb_instance"),
        [
            pytest.param(
                TEST_ASYNC_CALLBACK, TriggererCallback(callback_def=TEST_ASYNC_CALLBACK), id="triggerer"
            ),
            pytest.param(
                TEST_SYNC_CALLBACK,
                ExecutorCallback(
                    callback_def=TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH
                ),
                id="executor",
            ),
        ],
    )
    def test_create_from_sdk_def(self, callback_def, expected_cb_instance):
        returned_cb = Callback.create_from_sdk_def(callback_def)
        assert isinstance(returned_cb, type(expected_cb_instance))
        assert returned_cb.data == expected_cb_instance.data

    def test_create_from_sdk_def_unknown_type(self):
        """Test that unknown callback type raises ValueError"""

        class UnknownCallback:
            pass

        unknown_callback = UnknownCallback()

        with pytest.raises(ValueError, match="Cannot handle Callback of type"):
            Callback.create_from_sdk_def(unknown_callback)

    def test_get_metric_info(self):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK, prefix="deadline_alerts", dag_id=TEST_DAG_ID)
        callback.data["kwargs"] = {"context": {"dag_id": TEST_DAG_ID}, "email": "test@example.com"}
        metric_info = callback.get_metric_info(CallbackState.SUCCESS, "0")

        assert metric_info["stat"] == "deadline_alerts.callback_success"
        assert metric_info["tags"] == {
            "result": "0",
            "path": TEST_ASYNC_CALLBACK.path,
            "kwargs": '{"email": "test@example.com"}',
            "dag_id": TEST_DAG_ID,
        }

    @pytest.mark.parametrize(
        "encoded_interval",
        [
            pytest.param(
                {serde.CLASSNAME: "datetime.timedelta", serde.VERSION: 2, serde.DATA: 3600.0}, id="current"
            ),
            pytest.param({"__type": "timedelta", "__var": 3600.0}, id="legacy"),
        ],
    )
    @pytest.mark.usefixtures("allow_tracked_kwarg")
    def test_metric_tags_and_repr_show_kwargs_readably(self, encoded_interval):
        kwargs = {
            "interval": encoded_interval,
            "pair": {serde.CLASSNAME: "builtins.tuple", serde.VERSION: 1, serde.DATA: [1, 2]},
            "tracked": {serde.CLASSNAME: _TRACKED_KWARG_NAME, serde.VERSION: 0, serde.DATA: {}},
        }
        expected = {"interval": "1:00:00", "pair": [1, 2], "tracked": f"{_TRACKED_KWARG_NAME}({{}})"}
        executor_callback = ExecutorCallback(
            SyncCallback(sync_callback, kwargs=kwargs), fetch_method=CallbackFetchMethod.IMPORT_PATH
        )
        triggerer_callback = TriggererCallback(AsyncCallback(async_callback, kwargs=kwargs))

        tags = executor_callback.get_metric_info(CallbackState.SUCCESS, "0")["tags"]

        assert tags["kwargs"] == json.dumps(expected, sort_keys=True)
        assert repr(executor_callback) == f"{executor_callback.data['path']}({expected}) on None executor"
        assert repr(triggerer_callback) == f"{triggerer_callback.data['path']}({expected}) on a triggerer"
        assert _TrackedKwarg.instantiated is False

    @pytest.mark.parametrize(
        ("data", "expected"),
        [
            pytest.param(float("nan"), "datetime.timedelta(nan)", id="nan"),
            pytest.param(float("inf"), "datetime.timedelta(inf)", id="inf"),
            pytest.param(float("-inf"), "datetime.timedelta(-inf)", id="-inf"),
            pytest.param(1e300, "datetime.timedelta(1e+300)", id="too-large"),
            pytest.param("an hour", "datetime.timedelta(an hour)", id="not-a-number"),
        ],
    )
    def test_metric_tags_and_repr_fall_back_for_unusable_intervals(self, data, expected):
        kwargs = {"interval": {serde.CLASSNAME: "datetime.timedelta", serde.VERSION: 2, serde.DATA: data}}
        callback = TriggererCallback(AsyncCallback(async_callback, kwargs=kwargs))

        tags = callback.get_metric_info(CallbackState.SUCCESS, "0")["tags"]

        assert tags["kwargs"] == json.dumps({"interval": expected})
        assert repr(callback) == f"{callback.data['path']}({ {'interval': expected} }) on a triggerer"

    def test_get_metric_info_includes_queue(self):
        """``queue`` is stored in ``self.data`` for a queued AsyncCallback, so it flows into metric tags."""
        queued_callback = AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS, queue="custom-queue")
        callback = TriggererCallback(queued_callback, prefix="deadline_alerts", dag_id=TEST_DAG_ID)

        metric_info = callback.get_metric_info(CallbackState.SUCCESS, "0")

        assert metric_info["tags"]["queue"] == "custom-queue"

    def test_get_metric_info_dict_values_are_stringified(self):
        """
        Regression for ``TypeError: unhashable type: 'dict'`` raised by OpenTelemetry's
        ``_view_instrument_match`` when callback metric tags contain dict/list values.

        OTel builds its aggregation key as ``frozenset(attributes.items())``; any tag
        value that isn't hashable (dict, list, set) crashes the triggerer when a
        callback completes — e.g., deadline async callbacks whose ``result`` is a dict.
        """
        callback = TriggererCallback(TEST_ASYNC_CALLBACK, prefix="deadline_alerts", dag_id=TEST_DAG_ID)
        callback.data["kwargs"] = {"context": {"dag_id": TEST_DAG_ID}, "nested": {"a": 1}}

        # ``result`` is a dict — exactly the case that surfaced in the deadline DAG.
        metric_info = callback.get_metric_info(CallbackState.SUCCESS, {"output": [1, 2], "code": 0})

        # Every tag value must be a primitive (str/int/float/bool/None) so OTel can hash it.
        for k, v in metric_info["tags"].items():
            assert isinstance(v, (str, int, float, bool)) or v is None, (
                f"Tag {k!r}={v!r} is type {type(v).__name__}; must be primitive for OTel."
            )
        # ``frozenset(attributes.items())`` must not raise.
        frozenset(metric_info["tags"].items())
        # Stringified tag values must be sorted so equivalent kwargs in different
        # insertion order collapse to one metric series (no needless cardinality split).
        assert metric_info["tags"]["result"] == '{"code": 0, "output": [1, 2]}'

    @pytest.mark.parametrize(
        ("team_name", "expect_tag"),
        [
            pytest.param("team_alpha", True, id="with_team"),
            pytest.param(None, False, id="without_team"),
        ],
    )
    def test_get_metric_info_includes_team_name(self, team_name, expect_tag):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK, prefix="deadline_alerts", dag_id=TEST_DAG_ID)
        metric_info = callback.get_metric_info(CallbackState.SUCCESS, "0", team_name=team_name)
        if expect_tag:
            assert metric_info["tags"]["team_name"] == team_name
        else:
            assert "team_name" not in metric_info["tags"]


class TestTriggererCallback:
    def test_polymorphic_serde(self, session):
        """Test that TriggererCallback can be serialized and deserialized"""
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        session.add(callback)
        session.commit()

        retrieved = session.scalar(select(Callback).where(Callback.id == callback.id))
        assert isinstance(retrieved, TriggererCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == TEST_ASYNC_CALLBACK.serialize()
        assert retrieved.state == CallbackState.SCHEDULED.value
        assert retrieved.output is None
        assert retrieved.priority_weight == 1
        assert retrieved.created_at is not None
        assert retrieved.trigger_id is None

    def test_queue(self, session):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        assert callback.state == CallbackState.SCHEDULED
        assert callback.trigger is None

        callback.queue(session=session)
        assert isinstance(callback.trigger, Trigger)
        assert callback.trigger.kwargs["callback_path"] == TEST_ASYNC_CALLBACK.path
        assert callback.trigger.kwargs["callback_kwargs"] == TEST_ASYNC_CALLBACK.kwargs
        assert callback.trigger.queue is None
        assert callback.state == CallbackState.QUEUED

    @pytest.mark.usefixtures("allow_tracked_kwarg")
    @pytest.mark.parametrize("encoding", ["current", "legacy"])
    def test_queue_leaves_kwargs_for_the_triggerer_to_decode(self, session, encoding):
        callback_def = _decode_callback_def(
            AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS), ENCODED_CALLBACK_KWARGS[encoding]
        )
        callback = TriggererCallback(callback_def)
        session.add(callback)
        session.commit()
        session.expunge_all()
        callback = session.get(Callback, callback.id)
        deadline_time = timezone.datetime(2025, 1, 1)
        context = {"deadline": {"id": "1", "deadline_time": deadline_time}}
        callback.data = {**callback.data, "kwargs": {**callback.data["kwargs"], "context": context}}

        callback.queue(session=session)

        assert _TrackedKwarg.instantiated is False

        # Decode the way the triggerer does before it creates the trigger.
        trigger_kwargs = Trigger._decrypt_kwargs(callback.trigger.encrypted_kwargs)
        trigger = CallbackTrigger(**{k: smart_decode_trigger_kwargs(v) for k, v in trigger_kwargs.items()})

        assert trigger.callback_path == callback_def.path
        assert trigger.callback_kwargs.pop("context") == context
        _assert_decoded(trigger.callback_kwargs)

    def test_queue_populates_trigger_queue(self, session):
        queued_callback = AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS, queue="custom-queue")
        callback = TriggererCallback(queued_callback)

        callback.queue(session=session)

        assert callback.trigger.queue == "custom-queue"

    @staticmethod
    def _queue_callback(session, *, has_bundle, has_team):
        from airflow.models.dagbundle import DagBundleModel
        from airflow.models.team import Team

        bundle = session.get(DagBundleModel, "testing")
        bundle.teams = [session.get(Team, "testing")] if has_team else []
        session.flush()

        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        callback.bundle_name = "testing" if has_bundle else None
        callback.queue(session=session)
        return callback

    @conf_vars({("core", "multi_team"): "True"})
    @pytest.mark.parametrize(
        ("has_bundle", "has_team", "expected_team_name"),
        [
            pytest.param(True, True, "testing", id="bundle-mapped-to-team"),
            pytest.param(True, False, None, id="bundle-without-team"),
            pytest.param(False, False, None, id="no-bundle"),
        ],
    )
    def test_queue_populates_trigger_team_name(
        self, session, testing_dag_bundle, testing_team, has_bundle, has_team, expected_team_name
    ):
        callback = self._queue_callback(session, has_bundle=has_bundle, has_team=has_team)
        assert callback.trigger.team_name == expected_team_name

    @conf_vars({("core", "multi_team"): "False"})
    def test_queue_skips_trigger_team_name_when_multi_team_disabled(
        self, session, testing_dag_bundle, testing_team
    ):
        callback = self._queue_callback(session, has_bundle=True, has_team=True)
        assert callback.trigger.team_name is None

    @pytest.mark.parametrize(
        ("event", "terminal_state"),
        [
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: "test_result"}),
                True,
                id="success_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.FAILED, PAYLOAD_BODY_KEY: "RuntimeError"}),
                True,
                id="failed_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING}),
                False,
                id="running_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.QUEUED, PAYLOAD_BODY_KEY: ""}),
                False,
                id="invalid_event",
            ),
            pytest.param(TriggerEvent({PAYLOAD_STATUS_KEY: "unknown_state"}), False, id="unknown_event"),
        ],
    )
    def test_handle_event(self, session, event, terminal_state):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        callback.queue(session=session)
        callback.handle_event(event, session)

        status = event.payload[PAYLOAD_STATUS_KEY]
        if status in set(CallbackState):
            assert callback.state == status
        else:
            assert callback.state == CallbackState.QUEUED

        if terminal_state:
            assert callback.trigger is None
            assert callback.output == event.payload[PAYLOAD_BODY_KEY]

    @pytest.mark.parametrize(
        ("multi_team", "team_name", "expect_tag"),
        [
            pytest.param("true", "team_alpha", True, id="with_team"),
            pytest.param("false", None, False, id="without_team"),
        ],
    )
    @patch("airflow.models.callback.stats.incr")
    def test_handle_event_emits_team_name(self, mock_incr, multi_team, team_name, expect_tag, session):
        """On a terminal event, callback_{status} carries team_name resolved from the bundle."""
        callback = TriggererCallback(TEST_ASYNC_CALLBACK, dag_id=TEST_DAG_ID)
        callback.bundle_name = "test_bundle"
        event = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: "0"})

        with (
            conf_vars({("core", "multi_team"): multi_team}),
            patch(
                "airflow.models.callback.DagBundleModel.get_team_name", return_value=team_name
            ) as mock_get_team_name,
        ):
            callback.handle_event(event, session)

        mock_incr.assert_called_once()
        _, kwargs = mock_incr.call_args
        if expect_tag:
            assert kwargs["tags"]["team_name"] == team_name
        else:
            mock_get_team_name.assert_not_called()
            assert "team_name" not in kwargs["tags"]


class TestExecutorCallback:
    def test_polymorphic_serde(self, session):
        """Test that ExecutorCallback can be serialized and deserialized"""
        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH)
        session.add(callback)
        session.commit()

        retrieved = session.scalar(select(Callback).where(Callback.id == callback.id))
        assert isinstance(retrieved, ExecutorCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == {**TEST_SYNC_CALLBACK.serialize(), CALLBACK_KWARGS_ENCODING_KEY: "serde"}
        assert retrieved.state == CallbackState.SCHEDULED.value
        assert retrieved.output is None
        assert retrieved.priority_weight == 1
        assert retrieved.created_at is not None
        assert retrieved.trigger_id is None

    def test_queue(self, session):
        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.DAG_ATTRIBUTE)
        assert callback.state == CallbackState.SCHEDULED

        callback.queue(session=session)
        assert callback.state == CallbackState.QUEUED

    @pytest.mark.usefixtures("allow_tracked_kwarg")
    @pytest.mark.parametrize("encoding", ["current", "legacy"])
    def test_workload_leaves_kwargs_for_the_callback_runner_to_decode(self, session, encoding):
        callback_def = _decode_callback_def(
            SyncCallback(recording_sync_callback, kwargs=TEST_CALLBACK_KWARGS),
            ENCODED_CALLBACK_KWARGS[encoding],
        )
        callback = ExecutorCallback(callback_def, fetch_method=CallbackFetchMethod.IMPORT_PATH)
        session.add(callback)
        session.commit()
        session.expunge_all()
        callback = session.get(Callback, callback.id)

        workload_callback = CallbackDTO.model_validate_json(
            CallbackDTO.model_validate(callback, from_attributes=True).model_dump_json()
        )

        assert _TrackedKwarg.instantiated is False
        assert workload_callback.data["kwargs"] == ENCODED_CALLBACK_KWARGS[encoding]
        assert workload_callback.data[CALLBACK_KWARGS_ENCODING_KEY] == "serde"

        success, error = execute_callback(
            callback_path=workload_callback.data["path"],
            callback_kwargs=workload_callback.data["kwargs"],
            dag_rel_path=Path("test.py"),
            bundle_path=None,
            log=structlog.get_logger(),
            callback_kwargs_encoding=workload_callback.data[CALLBACK_KWARGS_ENCODING_KEY],
        )

        assert (success, error) == (True, None)
        _assert_decoded(received_sync_kwargs)

    def test_kwargs_are_stored_encoded_with_their_encoding(self):
        callback = ExecutorCallback(
            SyncCallback(sync_callback, kwargs={"interval": timedelta(hours=1), "arg1": "value1"}),
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
        )

        assert callback.data["kwargs"] == serde.serialize({"interval": timedelta(hours=1), "arg1": "value1"})
        assert callback.data[CALLBACK_KWARGS_ENCODING_KEY] == "serde"
        assert (
            CALLBACK_KWARGS_ENCODING_KEY not in callback.get_metric_info(CallbackState.SUCCESS, "ok")["tags"]
        )

    def test_session_get_requires_uuid_not_str(self, session):
        """Filtering the UUID id column with a plain str breaks on SQLite, so
        callers must wrap with ``UUID(...)`` before querying."""
        from uuid import UUID

        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH)
        session.add(callback)
        session.commit()
        # ``id`` is filled by the ``uuid6.uuid7`` default at flush time, so it
        # is only safe to stringify *after* the commit.
        callback_id_str = str(callback.id)

        assert session.get(Callback, UUID(callback_id_str)) is not None


class TestDagProcessorCallback:
    def test_polymorphic_serde(self, session):
        callback_request = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            bundle_name="testing",
            bundle_version=None,
            filepath="test_on_failure_callback_dag.py",
            is_failure_callback=True,
            run_id="123",
        )
        callback = DagProcessorCallback(priority_weight=11, callback=callback_request)
        session.add(callback)
        session.commit()

        retrieved = session.scalar(select(Callback).where(Callback.id == callback.id))
        assert isinstance(retrieved, DagProcessorCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.DAG_ATTRIBUTE
        assert retrieved.bundle_name == "testing"
        assert retrieved.priority_weight == 11
        assert retrieved.state is None


class TestAcceptsContext:
    def test_true_when_var_keyword_present(self):
        def func_with_var_keyword(**kwargs):
            pass

        assert accepts_context(func_with_var_keyword) is True

    def test_true_when_context_param_present(self):
        def func_with_context(context, alert_type):
            pass

        assert accepts_context(func_with_context) is True

    def test_false_when_no_context_or_var_keyword(self):
        def func_without_context(a, b):
            pass

        assert accepts_context(func_without_context) is False

    def test_false_when_no_params(self):
        def func_no_params():
            pass

        assert accepts_context(func_no_params) is False

    def test_true_for_uninspectable_callable(self):
        with patch("airflow._shared.module_loading.inspect.signature", side_effect=ValueError):
            assert accepts_context(lambda: None) is True
