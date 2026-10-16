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
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.durable import storage as storage_module
from airflow.providers.common.ai.durable.journal import DurableJournal
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.compat.sdk import ObjectStoragePath

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def tmp_cache_path(tmp_path):
    """Return a file:// path to a temporary directory for journal files."""
    return f"file://{tmp_path.as_posix()}"


@pytest.fixture
def storage(tmp_cache_path):
    with patch("airflow.providers.common.ai.durable.storage._get_base_path", autospec=True) as mock_base:
        mock_base.return_value = ObjectStoragePath(tmp_cache_path)
        yield DurableStorage(dag_id="test_dag", task_id="my_task", run_id="run_1", map_index=-1)


ENTRY = {"name": "agent__model.request", "kind": "model", "fingerprint": "fp", "payload": {"parts": []}}


class TestDurableStorageInit:
    def test_cache_id_is_deterministic(self):
        """The same task identity always maps to the same cache file (so retries resume)."""
        a = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        b = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        assert a._cache_id == b._cache_id

    def test_cache_id_differs_by_map_index(self):
        base = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)
        mapped = DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=3)
        assert base._cache_id != mapped._cache_id

    def test_cache_id_no_collision_across_tasks(self):
        """Distinct (dag, task) pairs that concatenate to the same string must not
        share a cache file -- e.g. dag ``etl`` + task ``load_data`` vs dag
        ``etl_load`` + task ``data``. A plain ``_``-join aliased them, letting one
        task read, overwrite, or delete another task's durable cache."""
        a = DurableStorage(dag_id="etl", task_id="load_data", run_id="r")
        b = DurableStorage(dag_id="etl_load", task_id="data", run_id="r")
        assert a._cache_id != b._cache_id


class TestSaveLoad:
    def test_entry_roundtrips(self, storage):
        assert storage.save_step("k", ENTRY)

        assert storage.load_step("k") == ENTRY

    def test_a_retry_reads_what_the_previous_attempt_wrote(self, storage):
        storage.save_step("k", ENTRY)

        retry = DurableStorage(dag_id="test_dag", task_id="my_task", run_id="run_1", map_index=-1)

        assert retry.load_step("k") == ENTRY

    def test_missing_key_is_none(self, storage):
        assert storage.load_step("missing") is None

    def test_an_entry_that_cannot_be_encoded_is_skipped_and_the_rest_kept(self, storage):
        circular: dict = {}
        circular["self"] = circular
        storage.save_step("good", ENTRY)

        assert not storage.save_step("bad", {"payload": circular})
        assert storage.load_step("good") == ENTRY
        assert storage.load_step("bad") is None

    def test_a_non_dict_value_is_a_miss(self, storage):
        storage._load_cache()["k"] = "legacy string entry"

        assert storage.load_step("k") is None

    def test_every_save_rewrites_one_file(self, storage, tmp_path):
        storage.save_step("a", ENTRY)
        storage.save_step("b", ENTRY)

        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert set(json.loads(files[0].read_text())) == {"a", "b"}


class TestDeleteSteps:
    def test_deleting_the_last_steps_removes_the_file(self, storage, tmp_path):
        storage.save_step("a", ENTRY)

        storage.delete_steps(["a"])

        assert list(tmp_path.iterdir()) == []

    def test_other_steps_are_kept(self, storage):
        storage.save_step("a", ENTRY)
        storage.save_step("b", ENTRY)

        storage.delete_steps(["a"])

        assert storage.load_step("a") is None
        assert storage.load_step("b") == ENTRY

    def test_deleting_when_there_is_no_file_does_not_raise(self, storage):
        storage.delete_steps(["a"])


def store_down():
    return patch.object(DurableStorage, "_write", autospec=True, side_effect=OSError("store unavailable"))


class TestStoreFailures:
    def test_a_failed_write_skips_the_step_without_raising(self, storage):
        with store_down(), patch.object(storage_module, "log") as mock_log:
            assert not storage.save_step("k", ENTRY)

        assert storage.load_step("k") is None
        mock_log.warning.assert_called_once()

    def test_a_failed_write_keeps_the_previous_value_of_a_step(self, storage):
        storage.save_step("meta", {"high_water": 1})

        with store_down():
            assert not storage.save_step("meta", {"high_water": 2})

        assert storage.load_step("meta") == {"high_water": 1}

    def test_an_entry_that_cannot_be_encoded_keeps_the_previous_value_of_a_step(self, storage):
        storage.save_step("meta", {"high_water": 1})

        assert not storage.save_step("meta", {"high_water": object()})

        assert storage.load_step("meta") == {"high_water": 1}

    def test_a_skipped_step_is_not_written_by_a_later_save(self, storage, tmp_path):
        with store_down():
            storage.save_step("skipped", ENTRY)
        storage.save_step("written", ENTRY)

        (journal_file,) = tmp_path.iterdir()
        assert set(json.loads(journal_file.read_text())) == {"written"}

    def test_an_outage_after_the_work_succeeded_does_not_fail_the_task(self, storage):
        journal = DurableJournal(storage)
        first, second = journal.start_run(), journal.start_run()
        first.save(0, ENTRY)
        second.save(0, ENTRY)

        with store_down():
            journal.cleanup()

    def test_an_outage_does_not_replace_the_error_the_run_failed_with(self, storage):
        run = DurableJournal(storage).start_run()
        run.save(0, ENTRY)

        with store_down():
            run.fail(RuntimeError("the agent's own error"))


class TestUnreadableJournal:
    @pytest.mark.parametrize(
        "content",
        [
            pytest.param("{truncated", id="cut off mid-write"),
            pytest.param('["not", "a", "journal"]', id="not a JSON object"),
        ],
    )
    def test_a_corrupt_journal_is_reported_and_started_over(self, storage, content):
        storage._get_path().write_text(content)

        with patch.object(storage_module, "log") as mock_log:
            assert storage.load_step("k") is None
        mock_log.warning.assert_called_once()

        assert storage.save_step("k", ENTRY)
        assert storage.load_step("k") == ENTRY

    def test_a_journal_that_cannot_be_read_is_not_overwritten(self, storage):
        storage.save_step("k", ENTRY)
        attempt = DurableStorage(dag_id="test_dag", task_id="my_task", run_id="run_1", map_index=-1)

        with (
            patch.object(ObjectStoragePath, "read_text", side_effect=OSError("read timed out")),
            patch.object(storage_module, "log") as mock_log,
        ):
            assert attempt.load_step("k") is None
        mock_log.warning.assert_called_once()

        assert not attempt.save_step("new", ENTRY)
        attempt.delete_steps(["k"])

        retry = DurableStorage(dag_id="test_dag", task_id="my_task", run_id="run_1", map_index=-1)
        assert retry.load_step("k") == ENTRY
        assert retry.load_step("new") is None


class TestMissingConfig:
    def test_a_missing_cache_path_names_the_option(self):
        storage_module._get_base_path.cache_clear()
        try:
            with conf_vars({("common.ai", "durable_cache_path"): ""}):
                with pytest.raises(ValueError, match=r"\[common.ai\] durable_cache_path"):
                    DurableStorage(dag_id="d", task_id="t", run_id="r").load_step("k")
        finally:
            storage_module._get_base_path.cache_clear()
