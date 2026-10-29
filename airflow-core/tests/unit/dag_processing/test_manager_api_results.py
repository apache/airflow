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

import signal
import time
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, DagFileStat
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess, DagParseSource
from airflow.sdk import timezone


@pytest.mark.parametrize("outcome", ["parsed", "callback", "failed", "stale", "stale_failure"])
@mock.patch("airflow.settings.Session", autospec=True, side_effect=AssertionError("Unexpected DB session"))
def test_api_result_collection_does_not_create_database_session(
    session_factory, outcome, tmp_path, time_machine
):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=1, api_client=client)
    time_machine.shift(manager.stale_dag_threshold + 1)
    file = DagFileInfo(bundle_name="testing", rel_path=Path("a.py"), bundle_path=tmp_path)
    processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
    processor.id = uuid7()
    processor.dispatch_sequence = 1
    processor.is_ready = True
    processor.start_time = time.monotonic()
    processor.had_callbacks = outcome == "callback"
    processor.api_request_error = None
    processor.parse_source = DagParseSource(bundle_revision=uuid7())
    processor.parsing_result = (
        DagFileParsingResult(fileloc="a.py", serialized_dags=[]) if outcome in {"parsed", "stale"} else None
    )
    source_changed = outcome in {"stale", "stale_failure"}
    manager._bundle_refresh_generations[file.bundle_name] = 1 if source_changed else 0
    manager._processors[file] = processor

    manager._collect_results()

    session_factory.assert_not_called()
    processor.close.assert_called_once_with()
    assert not manager._processors
    if outcome in {"parsed", "failed"}:
        body = manager._pending_publications[file].body
        assert body.attempt_id == processor.id
        assert body.dispatch_sequence == processor.dispatch_sequence
        assert body.bundle_revision == processor.parse_source.bundle_revision
        assert body.import_errors == ({"a.py": mock.ANY} if outcome == "failed" else {})
        manager._publish_pending_results()
        client.publish_parse_result.assert_called_once()
    else:
        assert not manager._pending_publications
        client.publish_parse_result.assert_not_called()
    assert list(manager._file_queue) == ([file] if source_changed else [])
    assert manager._file_stats[file].run_count == (0 if outcome == "callback" or source_changed else 1)


@pytest.mark.parametrize("outcome", ["parsed", "no_result", "timeout"])
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
def test_parse_with_a_failed_api_request_is_reported_only_after_stale_dag_threshold(
    clock, time_machine, outcome
):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    clock.monotonic.return_value = 100.0
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=1, api_client=client, processor_timeout=10)
    file = DagFileInfo(bundle_name="testing", rel_path=Path("a.py"))
    last_published = timezone.utcnow()
    manager._file_stats[file] = DagFileStat(num_dags=1, last_finish_time=last_published)

    def finish_parse():
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.pid = 123
        processor.dispatch_sequence = 1
        processor.start_time = 89 if outcome == "timeout" else 99
        processor.is_ready = True
        processor.had_callbacks = False
        processor.api_request_error = "GetVariable (HTTP 503)"
        processor.parse_source = DagParseSource(bundle_revision=uuid7())
        processor.parsing_result = (
            DagFileParsingResult(fileloc="a.py", serialized_dags=[]) if outcome == "parsed" else None
        )
        manager._processors[file] = processor
        if outcome == "timeout":
            manager._kill_timed_out_processors()
            processor.kill.assert_called_once_with(signal.SIGKILL)
        else:
            manager._collect_results()
        processor.close.assert_called_once_with()

    time_machine.shift(timedelta(seconds=manager.stale_dag_threshold))
    finish_parse()

    assert not manager._pending_publications
    stat = manager._file_stats[file]
    assert stat.num_dags == 1
    assert stat.last_finish_time == last_published
    assert stat.last_attempt_time == timezone.utcnow()
    assert stat.run_count == 1

    time_machine.shift(1)
    finish_parse()

    body = manager._pending_publications[file].body
    assert body.serialized_dags == []
    assert (
        "an Execution API request failed while parsing: GetVariable (HTTP 503)" in body.import_errors["a.py"]
    )
