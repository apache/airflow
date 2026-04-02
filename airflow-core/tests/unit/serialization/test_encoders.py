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
from sqlalchemy import delete

from airflow.models.trigger import Trigger
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.serialization.encoders import encode_trigger
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.triggers.base import BaseEventTrigger

pytest.importorskip("airflow.providers.apache.kafka")
from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger

# Trigger fixtures covering primitive-only kwargs (FileDeleteTrigger) and
# non-primitive kwargs like tuple/dict (AwaitMessageTrigger).
_TRIGGER_PARAMS = [
    pytest.param(
        FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=5.0),
        id="primitive_kwargs_only",
    ),
    pytest.param(AwaitMessageTrigger(topics=()), id="empty_tuple"),
    pytest.param(
        AwaitMessageTrigger(topics=("fizz_buzz",), poll_timeout=1.0, commit_offset=True),
        id="single_topic_tuple",
    ),
    pytest.param(
        AwaitMessageTrigger(
            topics=["t1", "t2"],
            apply_function="my.module.func",
            apply_function_args=["a", "b"],
            apply_function_kwargs={"key": "value"},
            kafka_config_id="my_kafka",
            poll_interval=2,
            poll_timeout=3,
        ),
        id="all_non_primitive_kwargs",
    ),
]


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
        trigger = AwaitMessageTrigger(topics=())
        result = encode_trigger(trigger)

        assert (
            result["classpath"] == "airflow.providers.apache.kafka.triggers.await_message.AwaitMessageTrigger"
        )
        # tuple kwarg is wrapped by BaseSerialization
        assert result["kwargs"]["topics"] == {Encoding.TYPE: DAT.TUPLE, Encoding.VAR: []}
        # Primitives pass through as-is
        assert result["kwargs"]["poll_timeout"] == 1
        assert result["kwargs"]["commit_offset"] is True

    def test_encode_file_delete_trigger(self):
        """Primitive-only kwargs pass through without wrapping."""
        trigger = FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=10.0)
        result = encode_trigger(trigger)

        assert result["classpath"] == "airflow.providers.standard.triggers.file.FileDeleteTrigger"
        assert result["kwargs"]["filepath"] == "/tmp/test.txt"
        assert result["kwargs"]["poke_interval"] == 10.0

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_re_encode_is_idempotent(self, trigger):
        """Encoding the output of encode_trigger again must not double-wrap kwargs."""
        first = encode_trigger(trigger)
        second = encode_trigger(first)

        assert first == second

    @pytest.mark.parametrize("trigger", _TRIGGER_PARAMS)
    def test_multiple_round_trips_are_stable(self, trigger):
        """Encoding the same trigger dict many times remains idempotent."""
        result = encode_trigger(trigger)
        for _ in range(5):
            result = encode_trigger(result)

        assert result == encode_trigger(trigger)


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

        dag_hash = BaseEventTrigger.hash(classpath, dag_kwargs)

        trigger_row = Trigger(classpath=classpath, kwargs=dag_kwargs)
        session.add(trigger_row)
        session.flush()

        trigger_id = trigger_row.id
        session.expire(trigger_row)
        reloaded = session.get(Trigger, trigger_id)

        db_hash = BaseEventTrigger.hash(reloaded.classpath, reloaded.kwargs)

        assert dag_hash == db_hash
