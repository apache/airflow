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
from pathlib import Path
from unittest import mock

import httpx2
import pytest
from sqlalchemy import select
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.dag_parsing import DagParseResultBody, ParseSourceCode
from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorClaims, DagProcessorToken
from airflow.api_fastapi.execution_api.routes.dag_parsing import publish_parse_result
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager, DagFileStat
from airflow.dag_processing.processor import DagFileProcessorProcess, DagParseSource
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError
from airflow.sdk import DAG, timezone
from airflow.serialization.serialized_objects import LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars


@pytest.mark.db_test
@pytest.mark.parametrize("outcome", ["no_result", "timeout"])
@pytest.mark.parametrize("lose_acknowledgment", [False, True])
@conf_vars({("workers", "execution_api_retries"): "2"})
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
def test_failed_parse_publishes_an_error_and_ages_out_prior_dags(
    clock, session, time_machine, outcome, lose_acknowledgment
):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    clock.monotonic.return_value = 100.0
    suffix = uuid7().hex
    bundle_name = f"failed_parse_{suffix}"
    dag_id = f"failed_parse_{suffix}"
    session_id = uuid7()
    revision = uuid7()
    job = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    job.session_id = session_id
    job.registration_id = uuid7()
    job.bundle_names = [bundle_name]
    session.add_all([job, DagBundleModel(name=bundle_name, version="v1", parse_revision=revision)])
    session.flush()
    token = DagProcessorToken(
        id=session_id,
        claims=DagProcessorClaims(exp=1, job_id=job.id, dag_bundles=frozenset({bundle_name})),
    )
    dag = DAG(dag_id, schedule=None)
    dag.fileloc = "/worker/a.py"
    dag.relative_fileloc = "a.py"
    body = DagParseResultBody(
        attempt_id=uuid7(),
        dispatch_sequence=1,
        bundle_name=bundle_name,
        relative_fileloc="a.py",
        bundle_version="v1",
        bundle_revision=revision,
        parse_duration=0.1,
        serialized_dags=[LazyDeserializedDAG.from_dag(dag).data],
        source_codes={dag_id: ParseSourceCode(source_code="# captured source", language="python")},
    )
    publish_parse_result(job.id, body, session=session, token=token)
    assert session.get(DagModel, dag_id).is_stale is False
    previous_finish = timezone.utcnow()
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.get_bundles.return_value = []

    def publish(body):
        receipt = publish_parse_result(job.id, body, session=session, token=token)
        if lose_acknowledgment and client.publish_parse_result.call_count == 1:
            raise httpx2.ReadTimeout("Lost publication acknowledgment")
        return receipt

    client.publish_parse_result.side_effect = publish
    manager = DagFileProcessorManager(max_runs=1, api_client=client, processor_timeout=10)
    manager._dispatch_sequence = 2
    time_machine.shift(manager.stale_dag_threshold + 1)
    file = DagFileInfo(bundle_name=bundle_name, rel_path=Path("a.py"))
    previous_stat = DagFileStat(num_dags=1, last_finish_time=previous_finish)
    manager._file_stats[file] = previous_stat
    processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
    processor.id = uuid7()
    processor.pid = 123
    processor.dispatch_sequence = 2
    processor.start_time = 89 if outcome == "timeout" else 99
    processor.is_ready = True
    processor.had_callbacks = False
    processor.parsing_result = None
    processor.parse_source = DagParseSource(bundle_version="v1", bundle_revision=revision)
    manager._processors[file] = processor

    if outcome == "timeout":
        manager._kill_timed_out_processors()
        processor.kill.assert_called_once_with(signal.SIGKILL)
    else:
        manager._collect_results()
    processor.close.assert_called_once_with()
    assert not manager._processors
    pending = manager._pending_publications[file]
    assert pending.body.attempt_id == processor.id
    assert pending.body.dispatch_sequence == processor.dispatch_sequence
    assert manager._dispatch_sequence == processor.dispatch_sequence
    assert pending.body.bundle_name == bundle_name
    assert pending.body.relative_fileloc == "a.py"
    assert pending.body.bundle_version == "v1"
    assert pending.body.bundle_revision == revision
    assert pending.body.parse_duration == (11 if outcome == "timeout" else 1)
    assert pending.body.serialized_dags == []
    assert pending.body.import_errors == {"a.py": mock.ANY}
    assert 0 < len(pending.body.import_errors["a.py"]) < 2048
    assert manager._file_stats[file] is previous_stat

    manager._publish_pending_results()
    session.expire_all()
    checkpoint = session.scalar(select(DagParseCheckpoint).where(DagParseCheckpoint.job_id == job.id))
    accepted_at = checkpoint.accepted_at
    if lose_acknowledgment:
        assert manager._pending_publications[file] is pending
        assert manager._file_stats[file] is previous_stat
        clock.monotonic.return_value = pending.next_attempt_time
        time_machine.shift(10)
        manager._publish_pending_results()
        session.expire_all()
        assert checkpoint.accepted_at == accepted_at

    assert not manager._pending_publications
    assert manager._file_stats[file].num_dags == 0
    assert manager._file_stats[file].import_errors == 1
    assert manager._file_stats[file].run_count == 1
    assert session.get(DagModel, dag_id).is_stale is True
    error = session.scalar(select(ParseImportError).where(ParseImportError.bundle_name == bundle_name))
    assert error is not None
    assert error.filename == "a.py"
    assert error.stacktrace == pending.body.import_errors["a.py"]
    assert checkpoint.attempt_id == processor.id
    assert checkpoint.dispatch_sequence == processor.dispatch_sequence
    assert client.publish_parse_result.call_args_list == [mock.call(pending.body)] * (
        2 if lose_acknowledgment else 1
    )


@pytest.mark.parametrize("outcome", ["callback", "refreshed"])
@mock.patch("airflow.settings.Session", autospec=True, side_effect=AssertionError("Unexpected DB session"))
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
def test_timeout_does_not_publish_for_callbacks_or_obsolete_sources(clock, session_factory, outcome):
    clock.monotonic.return_value = 100.0
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.get_bundles.return_value = []
    manager = DagFileProcessorManager(max_runs=1, api_client=client, processor_timeout=10)
    file = DagFileInfo(bundle_name="testing", rel_path=Path("a.py"))
    processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
    processor.id = uuid7()
    processor.pid = 123
    processor.dispatch_sequence = 1
    processor.start_time = 89
    processor.had_callbacks = outcome == "callback"
    processor.parsing_result = None
    processor.parse_source = DagParseSource(bundle_revision=uuid7())
    manager._processors[file] = processor
    manager._bundle_refresh_generations[file.bundle_name] = 1 if outcome == "refreshed" else 0

    manager._kill_timed_out_processors()
    manager._publish_pending_results()

    processor.kill.assert_called_once_with(signal.SIGKILL)
    processor.close.assert_called_once_with()
    assert not manager._processors
    assert not manager._pending_publications
    client.publish_parse_result.assert_not_called()
    session_factory.assert_not_called()
    assert list(manager._file_queue) == ([file] if outcome == "refreshed" else [])
