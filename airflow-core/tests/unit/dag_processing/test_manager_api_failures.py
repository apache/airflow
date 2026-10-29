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

from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleStateResponse,
    DagParseResultBody,
    ParseSourceCode,
)
from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorClaims, DagProcessorToken
from airflow.api_fastapi.execution_api.routes.dag_parsing import publish_parse_result
from airflow.dag_processing.api_client import DagProcessorAPIClient, DagProcessorRegistrationRetired
from airflow.dag_processing.manager import (
    DagFileInfo,
    DagFileProcessorManager,
    DagFileStat,
    PendingDagPublication,
)
from airflow.dag_processing.processor import DagFileProcessorProcess, DagParseSource
from airflow.jobs.job import Job, JobState
from airflow.models.dag import DagModel
from airflow.models.dag_processor_publication_receipt import DagProcessorPublicationReceipt
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
    bundle = DagBundleModel(name=bundle_name, version="v1")
    bundle.parse_revision = revision
    session.add_all([job, bundle])
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
        source_codes=[ParseSourceCode(source_code="# captured source", language="python")],
        dag_source_indexes={dag_id: 0},
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
    processor.api_request_error = None
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
    receipt = session.scalar(
        select(DagProcessorPublicationReceipt).where(DagProcessorPublicationReceipt.job_id == job.id)
    )
    accepted_at = receipt.accepted_at
    if lose_acknowledgment:
        assert manager._pending_publications[file] is pending
        assert manager._file_stats[file] is previous_stat
        clock.monotonic.return_value = pending.next_attempt_time
        time_machine.shift(10)
        manager._publish_pending_results()
        session.expire_all()
        assert receipt.accepted_at == accepted_at

    assert not manager._pending_publications
    assert manager._file_stats[file].num_dags == 0
    assert manager._file_stats[file].import_errors == 1
    assert manager._file_stats[file].run_count == 1
    assert session.get(DagModel, dag_id).is_stale is True
    error = session.scalar(select(ParseImportError).where(ParseImportError.bundle_name == bundle_name))
    assert error is not None
    assert error.filename == "a.py"
    assert error.stacktrace == pending.body.import_errors["a.py"]
    assert receipt.attempt_id == processor.id
    assert receipt.dispatch_sequence == processor.dispatch_sequence
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
    processor.api_request_error = None
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


@pytest.mark.parametrize("outcome", ["no_result", "timeout"])
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
def test_failed_parse_with_unrepresentable_diagnostic_does_not_stop_other_files(clock, time_machine, outcome):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    clock.monotonic.return_value = 100.0
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.get_bundles.return_value = []
    manager = DagFileProcessorManager(max_runs=1, api_client=client, processor_timeout=10)
    time_machine.shift(manager.stale_dag_threshold + 1)
    long_file, short_file = [
        DagFileInfo(bundle_name="testing", rel_path=Path(path))
        for path in ("directory/" * 110 + "a.py", "b.py")
    ]
    manager._file_stats[long_file] = DagFileStat(num_dags=1)
    for file in (long_file, short_file):
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.pid = 123
        processor.dispatch_sequence = 1
        processor.start_time = 89
        processor.is_ready = True
        processor.had_callbacks = False
        processor.api_request_error = None
        processor.parsing_result = None
        processor.parse_source = DagParseSource()
        manager._processors[file] = processor
    processes = list(manager._processors.values())

    if outcome == "timeout":
        manager._kill_timed_out_processors()
    else:
        manager._collect_results()
    manager._publish_pending_results()

    assert not manager._processors
    assert not manager._pending_publications
    assert manager._file_stats[long_file].num_dags == 1
    assert manager._file_stats[long_file].last_finish_time is None
    assert manager._file_stats[long_file].last_attempt_time is not None
    assert manager._file_stats[short_file].import_errors == 1
    client.publish_parse_result.assert_called_once()
    assert client.publish_parse_result.call_args.args[0].relative_fileloc == "b.py"
    for processor in processes:
        processor.close.assert_called_once_with()


def test_startup_catalog_read_waits_out_an_unavailable_api():
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.get_bundles.return_value = [
        DagBundleStateResponse(name="a", version=None, last_refreshed=None, revision=None, team_name=None)
    ]
    manager = DagFileProcessorManager(max_runs=1, api_client=client, bundle_names_to_parse=["a"])

    manager.sync_bundles()

    client.get_bundles.assert_called_once_with(retry=True)


@mock.patch.object(DagFileProcessorManager, "after_run", autospec=True)
@mock.patch.object(DagFileProcessorManager, "_run_parsing_loop", autospec=True)
@mock.patch.object(DagFileProcessorManager, "before_run", autospec=True)
def test_only_the_parsing_loop_retries_without_waiting(before_run, run_parsing_loop, after_run, tmp_path):
    with DagProcessorAPIClient(
        base_url="http://unused/", token_file=tmp_path / "processor.jwt", hostname="processor-1"
    ) as client:
        retries_at_once = []
        for phase in (before_run, run_parsing_loop, after_run):
            phase.side_effect = lambda _: retries_at_once.append(client._retry_without_waiting.get())

        DagFileProcessorManager(max_runs=1, api_client=client).run()

    assert retries_at_once == [False, True, False]


@pytest.mark.parametrize("outcome", ["no_result", "timeout"])
@pytest.mark.parametrize("published", [True, False], ids=["published", "after-restart"])
@mock.patch("airflow.dag_processing.manager.time", autospec=True)
def test_failed_parse_keeps_dags_until_the_failure_outlasts_stale_dag_threshold(
    clock, time_machine, published, outcome
):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    clock.monotonic.return_value = 100.0
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    manager = DagFileProcessorManager(max_runs=1, api_client=client, processor_timeout=10)
    file = DagFileInfo(bundle_name="testing", rel_path=Path("a.py"))
    last_published = timezone.utcnow() if published else None
    manager._file_stats[file] = DagFileStat(num_dags=1, last_finish_time=last_published)

    def fail_parse():
        processor = mock.create_autospec(DagFileProcessorProcess, instance=True)
        processor.id = uuid7()
        processor.pid = 123
        processor.dispatch_sequence = 1
        processor.start_time = 89 if outcome == "timeout" else 99
        processor.is_ready = True
        processor.had_callbacks = False
        processor.api_request_error = None
        processor.parsing_result = None
        processor.parse_source = DagParseSource()
        manager._processors[file] = processor
        if outcome == "timeout":
            manager._kill_timed_out_processors()
        else:
            manager._collect_results()

    time_machine.shift(manager.stale_dag_threshold)
    fail_parse()

    assert not manager._pending_publications
    assert manager._file_stats[file].num_dags == 1
    assert manager._file_stats[file].last_finish_time == last_published
    assert manager._file_stats[file].last_attempt_time == timezone.utcnow()

    time_machine.shift(1)
    fail_parse()

    assert manager._pending_publications[file].body.import_errors == {"a.py": mock.ANY}


def test_retired_registration_stops_publication():
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.publish_parse_result.side_effect = DagProcessorRegistrationRetired("Restart required")
    manager = DagFileProcessorManager(max_runs=1, api_client=client)
    for name in ("a.py", "b.py"):
        file = DagFileInfo(bundle_name="testing", rel_path=Path(name))
        manager._pending_publications[file] = PendingDagPublication(
            body=mock.create_autospec(DagParseResultBody, instance=True),
            stat=DagFileStat(),
            refresh_generation=0,
        )

    with pytest.raises(DagProcessorRegistrationRetired):
        manager._publish_pending_results()

    client.publish_parse_result.assert_called_once()
    assert len(manager._pending_publications) == 2
