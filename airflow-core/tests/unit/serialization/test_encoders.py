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

import datetime
import json
import pathlib
from enum import Enum
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import delete

from airflow.models.trigger import Trigger
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.serialization.encoders import encode_trigger
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding, stringify_encoding_keys
from airflow.triggers.base import BaseEventTrigger


class _CallableKwargsTrigger(BaseEventTrigger):
    """Mock trigger whose kwargs include non-primitive types (tuples, dicts, lists).

    This exercises the same serialization edge case as real provider triggers
    (e.g. Kafka's AwaitMessageTrigger) that pass callable-style kwargs, without
    requiring the provider to be installed.
    """

    def __init__(
        self,
        topics: tuple[str, ...] | list[str] = (),
        apply_function: str | None = None,
        apply_function_args: list[Any] | None = None,
        apply_function_kwargs: dict[str, Any] | None = None,
        poll_timeout: float = 1,
    ) -> None:
        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}
        self.poll_timeout = poll_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "topics": self.topics,
                "apply_function": self.apply_function,
                "apply_function_args": self.apply_function_args,
                "apply_function_kwargs": self.apply_function_kwargs,
                "poll_timeout": self.poll_timeout,
            },
        )

    run = AsyncMock()


class _GenericKwargsTrigger(BaseEventTrigger):
    """Mock trigger that forwards arbitrary kwargs verbatim.

    Lets each fixture pin a single kwarg shape at the top level of the trigger's
    kwargs, exercising the wrapper that ``BaseSerialization.serialize`` produces
    for that specific type.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._kwargs = kwargs

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            dict(self._kwargs),
        )

    run = AsyncMock()


class _StrColor(str, Enum):
    """str-Enum value form that ``BaseSerialization`` collapses to ``var.value``."""

    RED = "red"


# Trigger fixtures covering primitive-only kwargs (FileDeleteTrigger) and
# non-primitive kwargs like tuple/dict (_CallableKwargsTrigger).
_TRIGGER_PARAMS = [
    pytest.param(
        FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=5.0),
        id="primitive_kwargs_only",
    ),
    pytest.param(_CallableKwargsTrigger(topics=()), id="empty_tuple"),
    pytest.param(
        _CallableKwargsTrigger(topics=("fizz_buzz",), poll_timeout=1.0),
        id="single_topic_tuple",
    ),
    pytest.param(
        _CallableKwargsTrigger(
            topics=["t1", "t2"],
            apply_function="my.module.func",
            apply_function_args=["a", "b"],
            apply_function_kwargs={"key": "value"},
            poll_timeout=3,
        ),
        id="all_non_primitive_kwargs",
    ),
    pytest.param(
        _GenericKwargsTrigger(
            moment=datetime.datetime(2026, 1, 15, 12, 30, tzinfo=datetime.timezone.utc),
        ),
        id="datetime_kwarg",
    ),
    pytest.param(
        _GenericKwargsTrigger(delta=datetime.timedelta(hours=2, minutes=30, seconds=15)),
        id="timedelta_kwarg",
    ),
    pytest.param(
        _GenericKwargsTrigger(
            mapping={"outer": {"inner": [1, 2, 3]}, "sibling": ["a", "b"]},
            nested_records={"items": [{"k": "v"}, {"k2": "v2"}]},
            tuple_of_tuples=(("a", "b"), ("c", "d")),
        ),
        id="nested_containers",
    ),
    pytest.param(
        _GenericKwargsTrigger(
            tags={"a", "b", "c"},
            frozen=frozenset(["x", "y"]),
        ),
        id="set_and_frozenset_kwargs",
    ),
    pytest.param(
        _GenericKwargsTrigger(
            none_val=None,
            zero=0,
            empty_str="",
            false_val=False,
            empty_list=[],
            empty_dict={},
        ),
        id="falsy_primitives",
    ),
    pytest.param(
        _GenericKwargsTrigger(color=_StrColor.RED),
        id="str_enum_kwarg",
    ),
    pytest.param(
        _GenericKwargsTrigger(path=pathlib.Path("/tmp/test/file.txt")),
        id="path_kwarg",
    ),
]


def _assert_fully_serialized(encoded_kwargs: dict[str, Any]) -> None:
    """Assert encoded kwargs are fully JSON-safe and not immediately re-wrapped.

    Directly enforces the contract :func:`Trigger.encrypt_kwargs` relies on at
    ``json.dumps``: every value in ``encode_trigger`` output must be JSON-safe,
    including values that bypass ``BaseSerialization.serialize`` via the
    wrapper-detection early-return in ``_ensure_serialized``.
    """
    json.dumps(stringify_encoding_keys(encoded_kwargs))

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            if Encoding.TYPE in node:
                inner = node.get(Encoding.VAR)
                if isinstance(inner, dict) and Encoding.TYPE in inner:
                    assert node[Encoding.TYPE] != inner[Encoding.TYPE], (
                        f"double-wrapped value: wrapper type "
                        f"{node[Encoding.TYPE]!r} appears in both layers: {node!r}"
                    )
                _walk(inner)
            else:
                for v in node.values():
                    _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(encoded_kwargs)


class TestEncodeTrigger:
    """Tests for encode_trigger round-trip correctness.

    When a serialized DAG with asset-watcher triggers is re-serialized
    (e.g. in ``add_asset_trigger_references``), ``encode_trigger`` receives
    a dict whose kwargs already contain wrapped values like
    ``{__type: tuple, __var: [...]}``.  The fix ensures these are unwrapped
    before re-serialization to prevent double-wrapping.
    """

    def test_encode_from_trigger_object(self):
        """Non-primitive kwargs are properly serialized from a trigger object."""
        trigger = _CallableKwargsTrigger(topics=())
        result = encode_trigger(trigger)

        assert result["classpath"].endswith("_CallableKwargsTrigger")
        # tuple kwarg is wrapped by BaseSerialization
        assert result["kwargs"]["topics"] == {Encoding.TYPE: DAT.TUPLE, Encoding.VAR: []}
        # Primitives pass through as-is
        assert result["kwargs"]["poll_timeout"] == 1

    def test_encode_file_delete_trigger(self):
        """Primitive-only kwargs pass through without wrapping."""
        trigger = FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=10.0)
        result = encode_trigger(trigger)

        assert result["classpath"] == "airflow.providers.standard.triggers.file.FileDeleteTrigger"
        assert result["kwargs"]["filepath"] == "/tmp/test.txt"
        assert result["kwargs"]["poke_interval"] == 10.0

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_encoded_kwargs_are_fully_json_serializable(self, trigger):
        """encode_trigger output must satisfy the encrypt_kwargs JSON contract."""
        encoded = encode_trigger(trigger)
        _assert_fully_serialized(encoded["kwargs"])

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_re_encode_is_idempotent(self, trigger):
        """Encoding the output of encode_trigger again must not double-wrap kwargs."""
        first = encode_trigger(trigger)
        second = encode_trigger(first)

        assert first == second
        _assert_fully_serialized(second["kwargs"])

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_multiple_round_trips_are_stable(self, trigger):
        """Encoding the same trigger dict many times remains idempotent."""
        result = encode_trigger(trigger)
        for _ in range(5):
            result = encode_trigger(result)

        assert result == encode_trigger(trigger)
        _assert_fully_serialized(result["kwargs"])


def test_assert_fully_serialized_rejects_non_json_values():
    """The guard rejects non-JSON-safe values left in encode_trigger output."""
    with pytest.raises(TypeError):
        _assert_fully_serialized({"bad": datetime.datetime(2026, 1, 1)})


def test_assert_fully_serialized_rejects_double_wrap():
    """The guard rejects the re-wrap shape ``_ensure_serialized`` prevents."""
    double_wrapped = {
        "topics": {
            Encoding.TYPE: DAT.TUPLE,
            Encoding.VAR: {Encoding.TYPE: DAT.TUPLE, Encoding.VAR: []},
        }
    }
    with pytest.raises(AssertionError, match="double-wrapped"):
        _assert_fully_serialized(double_wrapped)


@pytest.mark.db_test
class TestTriggerHashConsistency:
    """Verify ``BaseEventTrigger.hash`` produces the same value for kwargs
    from the DAG-parsed path and kwargs read back from the database.

    This mirrors the comparison in
    ``AssetModelOperation.add_asset_trigger_references``
    (``airflow-core/src/airflow/dag_processing/collection.py``), where:

    * **DAG side** — ``BaseEventTrigger.hash(classpath, encode_trigger(watcher.trigger)["kwargs"])``
    * **DB side** — ``BaseEventTrigger.hash(trigger.classpath, trigger.kwargs)``
      where the ``Trigger`` row was persisted with ``encrypt_kwargs`` and
      read back via ``_decrypt_kwargs``.

    If the hashes diverge, the scheduler sees phantom diffs and keeps
    recreating trigger rows on every heartbeat.
    """

    @pytest.fixture(autouse=True)
    def _clean_triggers(self, session):
        session.execute(delete(Trigger))
        session.commit()
        yield
        session.execute(delete(Trigger))
        session.commit()

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_hash_matches_after_db_round_trip(self, trigger, session):
        """Hash from DAG-parsed kwargs equals hash from a DB-persisted Trigger."""
        encoded = encode_trigger(trigger)
        classpath = encoded["classpath"]
        dag_kwargs = encoded["kwargs"]

        _assert_fully_serialized(dag_kwargs)

        # DAG side hash — what add_asset_trigger_references computes
        dag_hash = BaseEventTrigger.hash(classpath, dag_kwargs)

        # Persist to DB (same as add_asset_trigger_references lines 1073-1074)
        trigger_row = Trigger(classpath=classpath, kwargs=dag_kwargs)
        session.add(trigger_row)
        session.flush()

        # Force a real DB read — expire the instance and re-select
        trigger_id = trigger_row.id
        session.expire(trigger_row)
        reloaded = session.get(Trigger, trigger_id)

        # DB side hash — what add_asset_trigger_references computes from ORM
        db_hash = BaseEventTrigger.hash(reloaded.classpath, reloaded.kwargs)

        assert dag_hash == db_hash

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_hash_matches_after_re_encode_and_db_round_trip(self, trigger, session):
        """Hash stays consistent when encode_trigger output is re-encoded
        (deserialized-DAG re-serialization path) before DB storage.
        """
        re_encoded = encode_trigger(encode_trigger(trigger))
        classpath = re_encoded["classpath"]
        dag_kwargs = re_encoded["kwargs"]

        _assert_fully_serialized(dag_kwargs)

        dag_hash = BaseEventTrigger.hash(classpath, dag_kwargs)

        trigger_row = Trigger(classpath=classpath, kwargs=dag_kwargs)
        session.add(trigger_row)
        session.flush()

        trigger_id = trigger_row.id
        session.expire(trigger_row)
        reloaded = session.get(Trigger, trigger_id)

        db_hash = BaseEventTrigger.hash(reloaded.classpath, reloaded.kwargs)

        assert dag_hash == db_hash
