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

import atexit
import json
import logging
import os
import time
from datetime import datetime, timezone
from fnmatch import fnmatch
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.kafka.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.sdk import AirflowPlugin, conf, hookimpl
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from confluent_kafka import Producer
    from sqlalchemy.orm import Session

    from airflow.models import DagRun, TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

CONFIG_SECTION = "kafka_event_producer"
SCHEMA_VERSION = 1


@lru_cache(maxsize=1)
def _dag_run_events_enabled() -> bool:
    return conf.getboolean(CONFIG_SECTION, "dag_run_events_enabled", fallback="False")


@lru_cache(maxsize=1)
def _task_instance_events_enabled() -> bool:
    return conf.getboolean(CONFIG_SECTION, "task_instance_events_enabled", fallback="False")


@lru_cache(maxsize=1)
def _get_kafka_config_id() -> str:
    return conf.get(CONFIG_SECTION, "kafka_config_id", fallback="").strip()


@lru_cache(maxsize=1)
def _get_topic() -> str:
    return conf.get(CONFIG_SECTION, "topic", fallback="airflow.events")


@lru_cache(maxsize=1)
def _get_source() -> str:
    source_from_conf = conf.get(CONFIG_SECTION, "source", fallback="")
    # Default to the current hostname, if un-set.
    return source_from_conf or get_hostname()


def _parse_filter_patterns_to_tuple(raw: str) -> tuple[str, ...]:
    return tuple(p.strip() for p in raw.split(",") if p.strip())


@lru_cache(maxsize=1)
def _get_dag_run_dag_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(conf.get(CONFIG_SECTION, "dag_run_dag_id_allowlist", fallback=""))


@lru_cache(maxsize=1)
def _get_dag_run_dag_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(conf.get(CONFIG_SECTION, "dag_run_dag_id_denylist", fallback=""))


@lru_cache(maxsize=1)
def _get_task_instance_dag_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_dag_id_allowlist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_dag_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_dag_id_denylist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_task_id_allowlist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_task_id_allowlist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_task_instance_task_id_denylist() -> tuple[str, ...]:
    return _parse_filter_patterns_to_tuple(
        conf.get(CONFIG_SECTION, "task_instance_task_id_denylist", fallback="")
    )


@lru_cache(maxsize=1)
def _get_topic_check_timeout() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_timeout", fallback="10")


@lru_cache(maxsize=1)
def _get_topic_check_retry_interval() -> int:
    return conf.getint(CONFIG_SECTION, "topic_check_retry_interval", fallback="60")


def _id_is_allowed(id_to_check: str, allowlist: tuple[str, ...], denylist: tuple[str, ...]) -> bool:
    """Deny takes precedence; empty allowlist means 'allow all'."""
    if denylist and any(fnmatch(id_to_check, id_pattern) for id_pattern in denylist):
        return False
    if allowlist and not any(fnmatch(id_to_check, id_pattern) for id_pattern in allowlist):
        return False
    return True


def _dag_run_event_allowed(dag_id: str) -> bool:
    return _id_is_allowed(
        dag_id,
        _get_dag_run_dag_id_allowlist(),
        _get_dag_run_dag_id_denylist(),
    )


def _task_instance_event_allowed(dag_id: str, task_id: str) -> bool:
    return _id_is_allowed(
        dag_id,
        _get_task_instance_dag_id_allowlist(),
        _get_task_instance_dag_id_denylist(),
    ) and _id_is_allowed(
        task_id,
        _get_task_instance_task_id_allowlist(),
        _get_task_instance_task_id_denylist(),
    )


# Plugin-owned producer and topic state for the lifetime of the process.
# The producer is built once via KafkaProducerHook and kept for the process lifetime;
# the topic flags track whether the topic exists on the broker and whether we're
# currently in a back-off window after a failed topic check.
_producer: Producer | None = None
_topic_exists: bool = False
_topic_check_retry_after: float = 0.0


def _reset_state_after_fork() -> None:
    """
    Reset the producer and the topic state in forked children processes.

    confluent_kafka.Producer is not fork-safe — its background thread and broker sockets
    do not survive ``os.fork``. The child re-initializes its own producer on first use
    and re-verifies the topic.
    """
    global _producer, _topic_exists, _topic_check_retry_after
    _producer = None
    _topic_exists = False
    _topic_check_retry_after = 0.0


os.register_at_fork(after_in_child=_reset_state_after_fork)


def _get_producer() -> Producer | None:
    """Build (once) and return the plugin's Kafka producer, or ``None`` on init failure."""
    global _producer
    if _producer is not None:
        return _producer

    # Deferred import: KafkaProducerHook pulls in confluent_kafka.Producer, which is heavy
    # and shouldn't be loaded by every Airflow process at plugin import time.
    from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

    try:
        hook_kwargs: dict[str, Any] = {}
        if _get_kafka_config_id():
            hook_kwargs["kafka_config_id"] = _get_kafka_config_id()
        producer = KafkaProducerHook(**hook_kwargs).get_producer()
    except Exception as exc:
        log.warning("Kafka event producer: failed to initialize producer (%s).", exc)
        return None

    atexit.register(_flush_producer_at_exit, producer)
    _producer = producer
    return _producer


def _check_topic_exists() -> bool:
    """
    Verify the configured topic exists on the broker, with a retry-after-failure cooldown.

    Once the topic has been confirmed, the result is kept for the process lifetime; until
    then, failed checks are retried on a configured interval.
    """
    global _topic_exists, _topic_check_retry_after
    if _topic_exists:
        return True
    if time.monotonic() < _topic_check_retry_after:
        return False

    producer = _get_producer()
    if producer is None:
        _topic_check_retry_after = time.monotonic() + _get_topic_check_retry_interval()
        return False

    try:
        topics = producer.list_topics(timeout=_get_topic_check_timeout()).topics
    except Exception as exc:
        log.warning(
            "Kafka event producer: topic check failed (%s). Will retry after %ds.",
            exc,
            _get_topic_check_retry_interval(),
        )
        _topic_check_retry_after = time.monotonic() + _get_topic_check_retry_interval()
        return False

    if _get_topic() not in topics:
        log.warning(
            "Kafka event producer: topic %r not found on the broker. Will retry after %ds. "
            "Create the topic on the broker to enable publishing.",
            _get_topic(),
            _get_topic_check_retry_interval(),
        )
        _topic_check_retry_after = time.monotonic() + _get_topic_check_retry_interval()
        return False

    log.info(
        "Kafka event producer attached: pid=%s source=%r topic=%r",
        os.getpid(),
        _get_source(),
        _get_topic(),
    )
    _topic_exists = True
    return True


def _flush_producer_at_exit(producer: Producer) -> None:
    try:
        producer.flush(5)
    except Exception:
        log.debug("Kafka event producer: error flushing producer on exit", exc_info=True)


def _on_delivery(err, _msg) -> None:
    if err is None:
        return
    log.warning("Kafka event producer: delivery failed: %s", err)
    # If the broker or local metadata says the topic is gone, the confirmation cached at
    # attach-time is stale. Flip it back so the next _check_topic_exists re-verifies with
    # the broker, gated by the same cooldown used elsewhere. Lets the plugin auto-recover
    # if the topic gets recreated instead of requiring a component restart.
    from confluent_kafka import KafkaError

    if err.code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
        global _topic_exists, _topic_check_retry_after
        _topic_exists = False
        _topic_check_retry_after = time.monotonic() + _get_topic_check_retry_interval()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# DagRun
def _produce_dr_message(event: str, dag_run: DagRun, msg: str) -> None:
    dag_id = dag_run.dag_id
    if not _dag_run_event_allowed(dag_id):
        return
    try:
        _produce_message(
            event,
            dag_id,
            dag_run.run_id,
            _get_dr_payload(dag_run, msg),
        )
    except Exception:
        log.exception("Kafka event producer: %s failed", event)


def _get_dr_payload(dag_run, msg) -> dict[str, Any]:
    return {
        "dag_id": getattr(dag_run, "dag_id", None),
        "run_id": getattr(dag_run, "run_id", None),
        "run_type": str(getattr(dag_run, "run_type", "") or ""),
        "logical_date": str(getattr(dag_run, "logical_date", "") or ""),
        "start_date": str(getattr(dag_run, "start_date", "") or ""),
        "end_date": str(getattr(dag_run, "end_date", "") or ""),
        "msg": msg or "",
    }


# TaskInstance
def _produce_ti_message(
    event: str,
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error=None,
) -> None:
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    if not _task_instance_event_allowed(dag_id, task_id):
        return
    try:
        _produce_message(
            event,
            dag_id,
            task_instance.run_id,
            _get_ti_payload(task_instance, previous_state, error=error),
        )
    except Exception:
        log.exception("Kafka event producer: %s failed", event)


def _get_ti_payload(ti, previous_state, error=None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": getattr(ti, "run_id", None),
        "try_number": getattr(ti, "try_number", None),
        "map_index": getattr(ti, "map_index", -1),
        "previous_state": str(previous_state) if previous_state is not None else None,
    }
    if error is not None:
        payload["error"] = str(error)
    return payload


def _produce_message(event: str, dag_id: str, run_id: str, payload: dict[str, Any]) -> None:
    if not _dag_run_events_enabled() and not _task_instance_events_enabled():
        return
    if not _check_topic_exists():
        return
    producer = _get_producer()
    if producer is None:
        return

    # When changing any field in the body or the payload,
    # also update the SCHEMA_VERSION to avoid breaking someone's workflow.
    body = {
        "schema_version": SCHEMA_VERSION,
        "source": _get_source(),
        "event": event,
        "timestamp": _now_iso(),
        **payload,
    }
    # Key groups all events from a single DagRun (DagRun + TaskInstance) into the same
    # Kafka partition. Within the partition, the order of the events is preserved,
    # e.g. dag_run.running > ti.running > ti.success > dag_run.success.
    key = f"{dag_id}/{run_id}".encode()
    try:
        producer.produce(
            _get_topic(),
            key=key,
            value=json.dumps(body, default=str).encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
    except Exception as ex:
        log.warning("Kafka event producer: failed to enqueue %s for %s/%s: %s", event, dag_id, run_id, ex)


# DagRunListener / TaskListener are pluggy hookimpl classes that plug into Airflow's
# listener API (``AirflowPlugin.listeners``). These classes listen for Airflow events
# and then produce a message for every event.
class DagRunListener:
    """Publishes DagRun state-change event messages to Kafka."""

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.running", dag_run, msg)

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.success", dag_run, msg)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        _produce_dr_message("dag_run.failed", dag_run, msg)


class TaskListener:
    """Publishes TaskInstance state-change event messages to Kafka."""

    if AIRFLOW_V_3_0_PLUS:

        @hookimpl
        def on_task_instance_running(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.running", previous_state, task_instance)

        @hookimpl
        def on_task_instance_success(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.success", previous_state, task_instance)

        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState,
            task_instance: RuntimeTaskInstance,
            error: None | str | BaseException,
        ):
            _produce_ti_message("task_instance.failed", previous_state, task_instance, error=error)

        @hookimpl
        def on_task_instance_skipped(
            self, previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance
        ):
            _produce_ti_message("task_instance.skipped", previous_state, task_instance)
    else:

        @hookimpl
        def on_task_instance_running(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.running", previous_state, task_instance)

        @hookimpl
        def on_task_instance_success(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.success", previous_state, task_instance)

        @hookimpl
        def on_task_instance_failed(  # type: ignore[misc]
            self,
            previous_state: TaskInstanceState,
            task_instance: TaskInstance,
            error: None | str | BaseException,
            session: Session,
        ):
            _produce_ti_message("task_instance.failed", previous_state, task_instance, error=error)

        @hookimpl
        def on_task_instance_skipped(  # type: ignore[misc]
            self, previous_state: TaskInstanceState, task_instance: TaskInstance, session: Session
        ):
            _produce_ti_message("task_instance.skipped", previous_state, task_instance)


def _get_enabled_listeners() -> list[object]:
    listeners: list[object] = []
    if _dag_run_events_enabled():
        listeners.append(DagRunListener())
    if _task_instance_events_enabled():
        listeners.append(TaskListener())
    return listeners


class KafkaEventProducerPlugin(AirflowPlugin):
    """Publishes Airflow DagRun and TaskInstance event messages to a defined Kafka topic."""

    name = "kafka_event_producer"
    listeners = _get_enabled_listeners()
