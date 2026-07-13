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
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from airflow.models import Connection
from airflow.providers.apache.kafka.plugins import listener
from airflow.providers.apache.kafka.plugins.listener import DagRunListener, TaskListener

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

# TaskInstance listener hooks required a ``session`` param for AF versions prior to 3.0.
_TI_SESSION_KWARG: dict = {} if AIRFLOW_V_3_0_PLUS else {"session": None}

_DAG_ID = "test_dag1"
_DAG_RUN_ID = "test_dag_run1"
_TASK_ID = "test_task1"
_RUN_TYPE = "manual"

_KAFKA_TOPIC = "airflow.events"
_SOURCE = "af-dev-env"

# The hook binds these names in its own namespace, so patch them there.
_PRODUCER_CLS = "airflow.providers.apache.kafka.hooks.produce.Producer"
_PRODUCER_HOOK_CLS = "airflow.providers.apache.kafka.hooks.produce.KafkaProducerHook"


@pytest.fixture(autouse=True)
def _default_kafka_conn(create_connection_without_db):
    # The listener builds its producer through ``KafkaProducerHook``, which requires
    # a resolvable ``kafka_default`` connection.
    create_connection_without_db(
        Connection(
            conn_id="kafka_default",
            conn_type="kafka",
            extra=json.dumps({"bootstrap.servers": "broker:29092"}),
        )
    )


@pytest.fixture(autouse=True)
def _clear_cached_values():
    """Invalidates caches before each test run."""
    # Reset config cache.
    listener._dag_run_events_enabled.cache_clear()
    listener._task_instance_events_enabled.cache_clear()
    listener._get_topic.cache_clear()
    listener._get_kafka_config_id.cache_clear()
    listener._get_source.cache_clear()
    listener._get_dag_run_dag_id_allowlist.cache_clear()
    listener._get_dag_run_dag_id_denylist.cache_clear()
    listener._get_task_instance_dag_id_allowlist.cache_clear()
    listener._get_task_instance_dag_id_denylist.cache_clear()
    listener._get_task_instance_task_id_allowlist.cache_clear()
    listener._get_task_instance_task_id_denylist.cache_clear()
    listener._get_topic_check_timeout.cache_clear()
    listener._get_topic_check_retry_interval.cache_clear()

    # Reset the module-level producer and topic state.
    listener._producer = None
    listener._topic_exists = False
    listener._topic_check_retry_after = 0.0


@pytest.fixture
def dr_mock():
    dr_mock = MagicMock()
    dr_mock.dag_id = _DAG_ID
    dr_mock.run_id = _DAG_RUN_ID
    dr_mock.run_type = _RUN_TYPE
    dr_mock.logical_date = None
    dr_mock.start_date = None
    dr_mock.end_date = None
    return dr_mock


@pytest.fixture
def ti_mock():
    ti_mock = MagicMock()
    ti_mock.dag_id = _DAG_ID
    ti_mock.run_id = _DAG_RUN_ID
    ti_mock.task_id = _TASK_ID
    ti_mock.try_number = 2
    ti_mock.map_index = -1
    return ti_mock


@pytest.fixture
def kafka_producer_mock():
    """Patches the Producer class used by KafkaProducerHook."""
    mock = MagicMock()
    # Make the topic check succeed for any configured topic name.
    mock.list_topics.return_value.topics.__contains__.return_value = True
    with patch(_PRODUCER_CLS, return_value=mock):
        yield mock


def _assert_common_message_fields(kafka_producer_mock, expected_event: str) -> dict:
    kafka_producer_mock.list_topics.assert_called_once()
    kafka_producer_mock.produce.assert_called_once()

    args = kafka_producer_mock.produce.call_args.args
    kwargs = kafka_producer_mock.produce.call_args.kwargs
    assert args == (_KAFKA_TOPIC,)
    assert kwargs["key"] == f"{_DAG_ID}/{_DAG_RUN_ID}".encode()

    body = json.loads(kwargs["value"].decode("utf-8"))
    assert body["schema_version"] == listener.SCHEMA_VERSION
    assert body["source"] == _SOURCE
    assert body["event"] == expected_event
    assert body["dag_id"] == _DAG_ID
    assert body["run_id"] == _DAG_RUN_ID
    assert "timestamp" in body
    return body


@pytest.mark.parametrize(
    ("configs", "expected_listener_classes"),
    [
        pytest.param(
            {
                ("kafka_listener", "dag_run_events_enabled"): "True",
                ("kafka_listener", "task_instance_events_enabled"): "True",
            },
            [DagRunListener, TaskListener],
            id="both_listeners_enabled",
        ),
        pytest.param(
            {
                ("kafka_listener", "dag_run_events_enabled"): "True",
            },
            [DagRunListener],
            id="only_dr_events",
        ),
        pytest.param(
            {
                ("kafka_listener", "task_instance_events_enabled"): "True",
            },
            [TaskListener],
            id="only_ti_events",
        ),
        pytest.param(
            {},
            [],
            id="listeners_disabled_by_default",
        ),
    ],
)
def test_get_enabled_listeners(configs, expected_listener_classes):
    with conf_vars(configs):
        registered_listeners = listener._get_enabled_listeners()
        # Both lists should have the same size.
        assert len(registered_listeners) == len(expected_listener_classes)

        # Iterate both lists and assert.
        assert [type(x) for x in registered_listeners] == expected_listener_classes


@pytest.mark.parametrize(
    ("dr_event_function", "expected_event"),
    [
        pytest.param(DagRunListener.on_dag_run_running, "dag_run.running", id="dr_running"),
        pytest.param(DagRunListener.on_dag_run_success, "dag_run.success", id="dr_success"),
        pytest.param(DagRunListener.on_dag_run_failed, "dag_run.failed", id="dr_failed"),
    ],
)
@conf_vars(
    {
        ("kafka_listener", "dag_run_events_enabled"): "True",
        ("kafka_listener", "topic"): _KAFKA_TOPIC,
        ("kafka_listener", "source"): _SOURCE,
    }
)
def test_produce_dr_message(
    dr_event_function: Callable,
    expected_event: str,
    dr_mock,
    kafka_producer_mock,
):
    dr_event_function(DagRunListener(), dag_run=dr_mock, msg=None)

    body = _assert_common_message_fields(kafka_producer_mock, expected_event)
    assert body["run_type"] == _RUN_TYPE
    assert body["logical_date"] == ""
    assert body["start_date"] == ""
    assert body["end_date"] == ""
    assert body["msg"] == ""


@pytest.mark.parametrize(
    ("ti_event_function", "expected_event", "extra_kwargs"),
    [
        pytest.param(TaskListener.on_task_instance_running, "task_instance.running", {}, id="ti_running"),
        pytest.param(TaskListener.on_task_instance_success, "task_instance.success", {}, id="ti_success"),
        pytest.param(TaskListener.on_task_instance_skipped, "task_instance.skipped", {}, id="ti_skipped"),
        pytest.param(
            TaskListener.on_task_instance_failed,
            "task_instance.failed",
            {"error": ValueError("boom")},
            id="ti_failed",
        ),
    ],
)
@conf_vars(
    {
        ("kafka_listener", "task_instance_events_enabled"): "True",
        ("kafka_listener", "topic"): _KAFKA_TOPIC,
        ("kafka_listener", "source"): _SOURCE,
    }
)
def test_produce_ti_message(
    ti_event_function: Callable,
    expected_event: str,
    extra_kwargs: dict,
    ti_mock,
    kafka_producer_mock,
):
    ti_event_function(
        TaskListener(),
        previous_state="running",
        task_instance=ti_mock,
        **extra_kwargs,
        **_TI_SESSION_KWARG,
    )

    body = _assert_common_message_fields(kafka_producer_mock, expected_event)
    assert body["task_id"] == _TASK_ID
    assert body["try_number"] == 2
    assert body["map_index"] == -1
    assert body["previous_state"] == "running"

    if "error" in extra_kwargs:
        assert body["error"] == str(extra_kwargs["error"])
    else:
        assert "error" not in body


class TestGetProducer:
    """Tests for the lifecycle and caching behavior of `_get_producer`."""

    @pytest.fixture(autouse=True)
    def _producer_conf(self):
        with conf_vars({("kafka_listener", "topic"): _KAFKA_TOPIC}):
            yield

    def test_producer_cached_on_success(self):
        kafka_producer_mock = MagicMock()

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock) as producer_cls_mock:
            producer1 = listener._get_producer()
            producer2 = listener._get_producer()

        assert producer1 is kafka_producer_mock
        assert producer2 is producer1
        producer_cls_mock.assert_called_once()

    def test_init_failure_warns_and_returns_none(self, monkeypatch):
        log_warning_mock = MagicMock()
        monkeypatch.setattr(listener.log, "warning", log_warning_mock)

        with patch(_PRODUCER_CLS, side_effect=ValueError("boom")):
            assert listener._get_producer() is None

        log_warning_mock.assert_called_once()
        warning_format, exc_arg = log_warning_mock.call_args.args
        assert "failed to initialize producer" in warning_format
        assert str(exc_arg) == "boom"

    def test_atexit_register(self, monkeypatch):
        """On success, _get_producer registers a flush handler with atexit so any
        pending messages are delivered to the broker when the process exits."""
        kafka_producer_mock = MagicMock()

        register_mock = MagicMock()
        monkeypatch.setattr(listener.atexit, "register", register_mock)

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            listener._get_producer()

        register_mock.assert_called_once_with(listener._flush_producer_at_exit, kafka_producer_mock)

    def test_hook_built_from_listener_config(self):
        """The listener section's ``kafka_config_id`` is forwarded to the hook."""
        hook_mock = MagicMock()

        with conf_vars({("kafka_listener", "kafka_config_id"): "kafka_events"}):
            with patch(_PRODUCER_HOOK_CLS, return_value=hook_mock) as hook_cls_mock:
                producer = listener._get_producer()

        hook_cls_mock.assert_called_once_with(kafka_config_id="kafka_events")
        assert producer is hook_mock.get_producer.return_value

    def test_hook_defaults_when_options_unset(self):
        """Without listener options the hook falls back to its own defaults."""
        hook_mock = MagicMock()

        with patch(_PRODUCER_HOOK_CLS, return_value=hook_mock) as hook_cls_mock:
            listener._get_producer()

        # Unset options must not be passed to the hook at all (e.g. as ""), so the
        # hook's own defaults (the "kafka_default" connection) apply.
        hook_cls_mock.assert_called_once_with()


class TestCheckTopicExists:
    """Tests for the topic check and its retry-after-failure cooldown."""

    @pytest.fixture(autouse=True)
    def _topic_conf(self):
        with conf_vars({("kafka_listener", "topic"): _KAFKA_TOPIC}):
            yield

    def test_topic_exists_kept_for_process_lifetime(self):
        kafka_producer_mock = MagicMock()
        kafka_producer_mock.list_topics.return_value.topics.__contains__.return_value = True

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            assert listener._check_topic_exists() is True
            # Once confirmed, the broker is not queried again.
            assert listener._check_topic_exists() is True

        kafka_producer_mock.list_topics.assert_called_once()

    def test_topic_doesnt_exist(self, monkeypatch):
        kafka_producer_mock = MagicMock()
        kafka_producer_mock.list_topics.return_value.topics.__contains__.return_value = False

        log_warning_mock = MagicMock()
        monkeypatch.setattr(listener.log, "warning", log_warning_mock)

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            assert listener._check_topic_exists() is False

        log_warning_mock.assert_called_once()
        # Format string + the two formatting args are passed positionally to log.warning.
        warning_format, topic_arg, retry_interval_arg = log_warning_mock.call_args.args
        assert "topic %r not found on the broker" in warning_format
        assert topic_arg == _KAFKA_TOPIC
        assert retry_interval_arg == listener._get_topic_check_retry_interval()

    def test_list_topics_failure_warns_and_sets_cooldown(self, monkeypatch):
        kafka_producer_mock = MagicMock()
        kafka_producer_mock.list_topics.side_effect = ValueError("broker down")

        log_warning_mock = MagicMock()
        monkeypatch.setattr(listener.log, "warning", log_warning_mock)

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            assert listener._check_topic_exists() is False
            # Within the cooldown window the check is not retried.
            assert listener._check_topic_exists() is False

        kafka_producer_mock.list_topics.assert_called_once()
        log_warning_mock.assert_called_once()
        assert "topic check failed" in log_warning_mock.call_args.args[0]

    def test_retried_after_failure_cooldown(self, monkeypatch):
        time_now = [1000.0]
        monkeypatch.setattr(listener.time, "monotonic", lambda: time_now[0])

        kafka_producer_mock = MagicMock()
        # The topic doesn't exist yet.
        kafka_producer_mock.list_topics.return_value.topics.__contains__.return_value = False

        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            # First check fails: the topic is missing.
            assert listener._check_topic_exists() is False
            assert kafka_producer_mock.list_topics.call_count == 1

            # The interval hasn't passed and the check isn't retried.
            time_now[0] += 1
            assert listener._check_topic_exists() is False
            assert kafka_producer_mock.list_topics.call_count == 1

            # Past the interval. The check is retried and succeeds this time.
            kafka_producer_mock.list_topics.return_value.topics.__contains__.return_value = True
            time_now[0] += listener._get_topic_check_retry_interval() + 1
            assert listener._check_topic_exists() is True
            assert kafka_producer_mock.list_topics.call_count == 2

    def test_producer_init_failure_sets_cooldown(self, monkeypatch):
        time_now = [1000.0]
        monkeypatch.setattr(listener.time, "monotonic", lambda: time_now[0])

        with patch(_PRODUCER_CLS, side_effect=ValueError("boom")) as producer_cls_mock:
            assert listener._check_topic_exists() is False
            assert producer_cls_mock.call_count == 1

            # The interval hasn't passed and the producer initialization isn't retried.
            time_now[0] += 1
            assert listener._check_topic_exists() is False
            assert producer_cls_mock.call_count == 1

            # Past the interval. The producer initialization is retried.
            time_now[0] += listener._get_topic_check_retry_interval() + 1
            assert listener._check_topic_exists() is False
            assert producer_cls_mock.call_count == 2

    @pytest.mark.parametrize(
        ("kafka_error", "topic_exists_expected"),
        [
            pytest.param(KafkaError.UNKNOWN_TOPIC_OR_PART, False, id="broker_says_topic_gone"),
            pytest.param(KafkaError._UNKNOWN_TOPIC, False, id="local_metadata_says_topic_gone"),
            pytest.param(KafkaError.BROKER_NOT_AVAILABLE, True, id="unrelated_delivery_error_ignored"),
        ],
    )
    def test_delivery_report_invalidates_topic_confirmation_only_on_missing_topic(
        self, monkeypatch, kafka_error, topic_exists_expected
    ):
        """
        If the delivery report failed and the topic is gone, ``_topic_exists`` must be reset so that the
        next produce triggers a fresh check. Any delivery error unrelated to the topic, should be ignored.
        """

        time_now = [1000.0]
        monkeypatch.setattr(listener.time, "monotonic", lambda: time_now[0])

        kafka_producer_mock = MagicMock()
        kafka_producer_mock.list_topics.return_value.topics.__contains__.return_value = True
        with patch(_PRODUCER_CLS, return_value=kafka_producer_mock):
            assert listener._check_topic_exists() is True
            assert listener._topic_exists is True

            err = MagicMock()
            err.code.return_value = kafka_error
            listener._on_delivery(err, MagicMock())

            assert listener._topic_exists is topic_exists_expected

            if not topic_exists_expected:
                # Cooldown holds off the immediate re-check.
                assert listener._check_topic_exists() is False
                # After the cooldown, the check re-verifies; the topic is still on the broker,
                # so confirmation is restored.
                time_now[0] += listener._get_topic_check_retry_interval() + 1
                assert listener._check_topic_exists() is True


class TestFilters:
    """Tests for the allowlist/denylist behavior of both DagRun and TaskInstance events."""

    @pytest.fixture(autouse=True)
    def _filters_conf(self):
        with conf_vars(
            {
                ("kafka_listener", "dag_run_events_enabled"): "True",
                ("kafka_listener", "task_instance_events_enabled"): "True",
                ("kafka_listener", "topic"): _KAFKA_TOPIC,
            }
        ):
            yield

    @pytest.mark.parametrize(
        ("id_to_check", "allowlist", "denylist", "expected_result"),
        [
            pytest.param("some_value", (), (), True, id="empty_lists_allow_all"),
            pytest.param("dag__1", ("dag_*",), (), True, id="allowlist_match"),
            pytest.param("dag1", ("dag2_*", "dag2"), (), False, id="allowlist_no_match"),
            pytest.param("task1_dev", (), ("task1_*",), False, id="denylist_match"),
            pytest.param("dag1_task1", ("dag1_*",), ("dag1_task1",), False, id="deny_beats_allow"),
        ],
    )
    def test_id_is_allowed(self, id_to_check, allowlist, denylist, expected_result):
        assert listener._id_is_allowed(id_to_check, allowlist, denylist) is expected_result

    @conf_vars(
        {
            ("kafka_listener", "task_instance_dag_id_denylist"): "dag1_*",
        }
    )
    def test_dag_run_filter_ignores_ti_lists(self):
        # The dag_id lists for DR events, are separate from the dag_id lists for TI events.
        # The dag_id denylist for task instances would match "dag1_task1" but DR filter must ignore it.
        assert listener._dag_run_event_allowed("dag1_task1") is True

    @conf_vars(
        {
            ("kafka_listener", "dag_run_dag_id_denylist"): "dag1_*",
        }
    )
    def test_task_instance_filter_ignores_dr_lists(self):
        # The dag_id lists for DR events, are separate from the dag_id lists for TI events.
        # DR denylist would match "dag1_task1" but TI filter must ignore it.
        assert listener._task_instance_event_allowed("dag1_task1", "load") is True

    @conf_vars(
        {
            ("kafka_listener", "task_instance_dag_id_allowlist"): "dag1_*",
            ("kafka_listener", "task_instance_task_id_denylist"): "*_cleanup",
        }
    )
    def test_task_instance_requires_both_dag_id_and_task_id(self):
        # dag_id passes (matches allowlist), task_id passes (no deny match).
        assert listener._task_instance_event_allowed("dag1_task1", "task2") is True
        # dag_id passes, task_id denied.
        assert listener._task_instance_event_allowed("dag1_task1", "task2_cleanup") is False
        # dag_id denied (no allowlist match), task_id passes.
        assert listener._task_instance_event_allowed("other_dag", "load") is False

    @conf_vars({("kafka_listener", "dag_run_dag_id_denylist"): _DAG_ID})
    def test_dr_filter_blocks_emission(self, dr_mock, kafka_producer_mock):
        DagRunListener().on_dag_run_running(dag_run=dr_mock, msg=None)
        kafka_producer_mock.produce.assert_not_called()

    @conf_vars({("kafka_listener", "task_instance_dag_id_denylist"): _DAG_ID})
    def test_ti_filter_blocks_emission_via_dag_id(self, ti_mock, kafka_producer_mock):
        TaskListener().on_task_instance_running(
            previous_state="queued", task_instance=ti_mock, **_TI_SESSION_KWARG
        )
        kafka_producer_mock.produce.assert_not_called()

    @conf_vars({("kafka_listener", "task_instance_task_id_denylist"): _TASK_ID})
    def test_ti_filter_blocks_emission_via_task_id(self, ti_mock, kafka_producer_mock):
        TaskListener().on_task_instance_running(
            previous_state="queued", task_instance=ti_mock, **_TI_SESSION_KWARG
        )
        kafka_producer_mock.produce.assert_not_called()
