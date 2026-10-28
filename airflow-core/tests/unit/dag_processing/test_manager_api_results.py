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

import time
from pathlib import Path
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess, DagParseSource


@pytest.mark.parametrize("outcome", ["parsed", "callback", "failed", "stale", "stale_failure"])
@mock.patch("airflow.settings.Session", autospec=True, side_effect=AssertionError("Unexpected DB session"))
def test_api_result_collection_does_not_create_database_session(session_factory, outcome, tmp_path):
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=1, api_client=client)
    file = DagFileInfo(bundle_name="testing", rel_path=Path("a.py"), bundle_path=tmp_path)
    processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
    processor.id = uuid7()
    processor.dispatch_sequence = 1
    processor.is_ready = True
    processor.start_time = time.monotonic()
    processor.had_callbacks = outcome == "callback"
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
