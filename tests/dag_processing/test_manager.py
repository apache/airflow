#
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

import io
import itertools
import json
import logging
import multiprocessing
import os
import pathlib
import random
import signal
import socket
import textwrap
import threading
import time
from collections import deque
from contextlib import suppress
from datetime import datetime, timedelta
from logging.config import dictConfig
from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest
import time_machine
from sqlalchemy import func
from uuid6 import uuid7

from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.manager import (
    DagFileInfo,
    DagFileProcessorAgent,
    DagFileProcessorManager,
    DagFileStat,
)
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.models import DAG, DagBag, DagModel, DbCallbackRequest
from airflow.models.asset import TaskOutletAssetReference
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import reap_process_group
from airflow.utils.session import create_session

from tests.models import TEST_DAGS_FOLDER
from tests_common.test_utils.compat import ParseImportError
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_callbacks,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

logger = logging.getLogger(__name__)
TEST_DAG_FOLDER = pathlib.Path(__file__).parents[1].resolve() / "dags"
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def _get_dag_file_paths(files: list[str]) -> list[DagFileInfo]:
    return [DagFileInfo(bundle_name="testing", path=f) for f in files]


class TestDagFileProcessorManager:
    @pytest.fixture(autouse=True)
    def _disable_examples(self):
        with conf_vars({("core", "load_examples"): "False"}):
            yield

    def setup_method(self):
        dictConfig(DEFAULT_LOGGING_CONFIG)
        clear_db_assets()
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_callbacks()
        clear_db_import_errors()

    def teardown_class(self):
        clear_db_assets()
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_callbacks()
        clear_db_import_errors()

    def mock_processor(self) -> DagFileProcessorProcess:
        proc = MagicMock()
        proc.create_time.return_value = time.time()
        proc.wait.return_value = 0
        ret = DagFileProcessorProcess(
            id=uuid7(),
            pid=1234,
            process=proc,
            stdin=io.BytesIO(),
            requests_fd=123,
        )
        ret._num_open_sockets = 0
        return ret

    @pytest.fixture
    def clear_parse_import_errors(self):
        clear_db_import_errors()

    @pytest.mark.usefixtures("clear_parse_import_errors")
    @conf_vars({("core", "load_examples"): "False"})
    def test_remove_file_clears_import_error(self, tmp_path, configure_testing_dag_bundle):
        path_to_parse = tmp_path / "temp_dag.py"

        # Generate original import error
        path_to_parse.write_text("an invalid airflow DAG")

        with configure_testing_dag_bundle(path_to_parse):
            manager = DagFileProcessorManager(
                max_runs=1,
                processor_timeout=365 * 86_400,
            )

            with create_session() as session:
                manager.run()

                import_errors = session.query(ParseImportError).all()
                assert len(import_errors) == 1

                path_to_parse.unlink()

                # Rerun the parser once the dag file has been removed
                manager.run()
                import_errors = session.query(ParseImportError).all()

                assert len(import_errors) == 0
                session.rollback()

    @conf_vars({("core", "load_examples"): "False"})
    def test_max_runs_when_no_files(self, tmp_path):
        with conf_vars({("core", "dags_folder"): str(tmp_path)}):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        # TODO: AIP-66 no asserts?

    def test_start_new_processes_with_same_filepath(self):
        """
        Test that when a processor already exist with a filepath, a new processor won't be created
        with that filepath. The filepath will just be removed from the list.
        """
        manager = DagFileProcessorManager(max_runs=1)

        file_1 = DagFileInfo(bundle_name="testing", path="file_1.py")
        file_2 = DagFileInfo(bundle_name="testing", path="file_2.py")
        file_3 = DagFileInfo(bundle_name="testing", path="file_3.py")
        manager._file_path_queue = deque([file_1, file_2, file_3])

        # Mock that only one processor exists. This processor runs with 'file_1'
        manager._processors[file_1] = MagicMock()
        # Start New Processes
        manager._start_new_processes()

        # Because of the config: '[scheduler] parsing_processes = 2'
        # verify that only one extra process is created
        # and since a processor with 'file_1' already exists,
        # even though it is first in '_file_path_queue'
        # a new processor is created with 'file_2' and not 'file_1'.

        assert file_1 in manager._processors.keys()
        assert file_2 in manager._processors.keys()
        assert deque([file_3]) == manager._file_path_queue

    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        """Ensure processors and file stats are removed when the file path is not in the new file paths"""
        manager = DagFileProcessorManager(max_runs=1)
        file = DagFileInfo(bundle_name="testing", path="missing_file.txt")

        manager._processors[file] = MagicMock()
        manager._file_stats[file] = DagFileStat()

        manager.set_file_paths(["abc.txt"])
        assert manager._processors == {}
        assert file not in manager._file_stats

    def test_set_file_paths_when_processor_file_path_is_in_new_file_paths(self):
        manager = DagFileProcessorManager(max_runs=1)
        file = DagFileInfo(bundle_name="testing", path="abc.txt")
        mock_processor = MagicMock()

        manager._processors[file] = mock_processor

        manager.set_file_paths([file])
        assert manager._processors == {file: mock_processor}

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "alphabetical"})
    def test_file_paths_in_queue_sorted_alphabetically(self):
        """Test dag files are sorted alphabetically"""
        file_names = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]
        dag_files = _get_dag_file_paths(file_names)
        ordered_dag_files = _get_dag_file_paths(sorted(file_names))

        manager = DagFileProcessorManager(max_runs=1)

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == deque()
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == deque(ordered_dag_files)

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "random_seeded_by_host"})
    def test_file_paths_in_queue_sorted_random_seeded_by_host(self):
        """Test files are randomly sorted and seeded by host name"""
        dag_files = _get_dag_file_paths(["file_3.py", "file_2.py", "file_4.py", "file_1.py"])

        manager = DagFileProcessorManager(max_runs=1)

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == deque()
        manager.prepare_file_path_queue()

        expected_order = deque(dag_files)
        random.Random(get_hostname()).shuffle(expected_order)
        assert manager._file_path_queue == expected_order

        # Verify running it again produces same order
        manager._file_paths = []
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == expected_order

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_sorted_by_modified_time(self, mock_getmtime):
        """Test files are sorted by modified time"""
        paths_with_mtime = {"file_3.py": 3.0, "file_2.py": 2.0, "file_4.py": 5.0, "file_1.py": 4.0}
        dag_files = _get_dag_file_paths(paths_with_mtime.keys())
        mock_getmtime.side_effect = list(paths_with_mtime.values())

        manager = DagFileProcessorManager(max_runs=1)

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == deque()
        manager.prepare_file_path_queue()
        ordered_files = _get_dag_file_paths(["file_4.py", "file_1.py", "file_3.py", "file_2.py"])
        assert manager._file_path_queue == deque(ordered_files)

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_excludes_missing_file(self, mock_getmtime):
        """Check that a file is not enqueued for processing if it has been deleted"""
        dag_files = _get_dag_file_paths(["file_3.py", "file_2.py", "file_4.py"])
        mock_getmtime.side_effect = [1.0, 2.0, FileNotFoundError()]

        manager = DagFileProcessorManager(max_runs=1)

        manager.set_file_paths(dag_files)
        manager.prepare_file_path_queue()

        ordered_files = _get_dag_file_paths(["file_2.py", "file_3.py"])
        assert manager._file_path_queue == deque(ordered_files)

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_add_new_file_to_parsing_queue(self, mock_getmtime):
        """Check that new file is added to parsing queue"""
        dag_files = _get_dag_file_paths(["file_1.py", "file_2.py", "file_3.py"])
        mock_getmtime.side_effect = [1.0, 2.0, 3.0]

        manager = DagFileProcessorManager(max_runs=1)

        manager.set_file_paths(dag_files)
        manager.prepare_file_path_queue()
        ordered_files = _get_dag_file_paths(["file_3.py", "file_2.py", "file_1.py"])
        assert manager._file_path_queue == deque(ordered_files)

        manager.set_file_paths([*dag_files, DagFileInfo(bundle_name="testing", path="file_4.py")])
        manager.add_new_file_path_to_queue()
        ordered_files = _get_dag_file_paths(["file_4.py", "file_3.py", "file_2.py", "file_1.py"])
        assert manager._file_path_queue == deque(ordered_files)

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_recently_modified_file_is_parsed_with_mtime_mode(self, mock_getmtime):
        """
        Test recently updated files are processed even if min_file_process_interval is not reached
        """
        freezed_base_time = timezone.datetime(2020, 1, 5, 0, 0, 0)
        initial_file_1_mtime = (freezed_base_time - timedelta(minutes=5)).timestamp()
        dag_file = DagFileInfo(bundle_name="testing", path="file_1.py")
        dag_files = [dag_file]
        mock_getmtime.side_effect = [initial_file_1_mtime]

        manager = DagFileProcessorManager(max_runs=3)

        # let's say the DAG was just parsed 10 seconds before the Freezed time
        last_finish_time = freezed_base_time - timedelta(seconds=10)
        manager._file_stats = {
            dag_file: DagFileStat(1, 0, last_finish_time, 1.0, 1, 1),
        }
        with time_machine.travel(freezed_base_time):
            manager.set_file_paths(dag_files)
            assert manager._file_path_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            manager.prepare_file_path_queue()
            assert manager._file_path_queue == deque()

        # Simulate the DAG modification by using modified_time which is greater
        # than the last_parse_time but still less than now - min_file_process_interval
        file_1_new_mtime = freezed_base_time - timedelta(seconds=5)
        file_1_new_mtime_ts = file_1_new_mtime.timestamp()
        with time_machine.travel(freezed_base_time):
            manager.set_file_paths(dag_files)
            assert manager._file_path_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            mock_getmtime.side_effect = [file_1_new_mtime_ts]
            manager.prepare_file_path_queue()
            # Check that file is added to the queue even though file was just recently passed
            assert manager._file_path_queue == deque(dag_files)
            assert last_finish_time < file_1_new_mtime
            assert (
                manager._file_process_interval
                > (freezed_base_time - manager._file_stats[dag_file].last_finish_time).total_seconds()
            )

    @pytest.mark.skip("AIP-66: parsing requests are not bundle aware yet")
    def test_file_paths_in_queue_sorted_by_priority(self):
        from airflow.models.dagbag import DagPriorityParsingRequest

        parsing_request = DagPriorityParsingRequest(fileloc="file_1.py")
        with create_session() as session:
            session.add(parsing_request)
            session.commit()

        """Test dag files are sorted by priority"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]

        manager = DagFileProcessorManager(dag_directory="directory", max_runs=1)

        manager.set_file_paths(dag_files)
        manager._file_path_queue = deque(["file_2.py", "file_3.py", "file_4.py", "file_1.py"])
        manager._refresh_requested_filelocs()
        assert manager._file_path_queue == deque(["file_1.py", "file_2.py", "file_3.py", "file_4.py"])
        with create_session() as session2:
            parsing_request_after = session2.query(DagPriorityParsingRequest).get(parsing_request.id)
        assert parsing_request_after is None

    def test_scan_stale_dags(self, testing_dag_bundle):
        """
        Ensure that DAGs are marked inactive when the file is parsed but the
        DagModel.last_parsed_time is not updated.
        """
        manager = DagFileProcessorManager(
            max_runs=1,
            processor_timeout=10 * 60,
        )

        test_dag_path = DagFileInfo(
            bundle_name="testing",
            path=str(TEST_DAG_FOLDER / "test_example_bash_operator.py"),
        )
        dagbag = DagBag(test_dag_path.path, read_dags_from_db=False, include_examples=False)

        with create_session() as session:
            # Add stale DAG to the DB
            dag = dagbag.get_dag("test_example_bash_operator")
            dag.last_parsed_time = timezone.utcnow()
            DAG.bulk_write_to_db("testing", None, [dag])
            SerializedDagModel.write_dag(dag)

            # Add DAG to the file_parsing_stats
            stat = DagFileStat(
                num_dags=1,
                import_errors=0,
                last_finish_time=timezone.utcnow() + timedelta(hours=1),
                last_duration=1,
                run_count=1,
                last_num_of_db_queries=1,
            )
            manager._file_paths = [test_dag_path]
            manager._file_stats[test_dag_path] = stat

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(DagModel.is_active, DagModel.fileloc == test_dag_path.path)
                .scalar()
            )
            assert active_dag_count == 1

            manager._scan_stale_dags()

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(DagModel.is_active, DagModel.fileloc == test_dag_path.path)
                .scalar()
            )
            assert active_dag_count == 0

            serialized_dag_count = (
                session.query(func.count(SerializedDagModel.dag_id))
                .filter(SerializedDagModel.dag_id == dag.dag_id)
                .scalar()
            )
            # Deactivating the DagModel should not delete the SerializedDagModel
            # SerializedDagModel gives history about Dags
            assert serialized_dag_count == 1

    def test_kill_timed_out_processors_kill(self):
        manager = DagFileProcessorManager(max_runs=1, processor_timeout=5)

        processor = self.mock_processor()
        processor._process.create_time.return_value = timezone.make_aware(datetime.min).timestamp()
        manager._processors = {DagFileInfo(bundle_name="testing", path="abc.txt"): processor}
        with mock.patch.object(type(processor), "kill") as mock_kill:
            manager._kill_timed_out_processors()
        mock_kill.assert_called_once_with(signal.SIGKILL)
        assert len(manager._processors) == 0

    def test_kill_timed_out_processors_no_kill(self):
        manager = DagFileProcessorManager(
            max_runs=1,
            processor_timeout=5,
        )

        processor = self.mock_processor()
        processor._process.create_time.return_value = timezone.make_aware(datetime.max).timestamp()
        manager._processors = {DagFileInfo(bundle_name="testing", path="abc.txt"): processor}
        with mock.patch.object(type(processor), "kill") as mock_kill:
            manager._kill_timed_out_processors()
        mock_kill.assert_not_called()

    @pytest.mark.parametrize(
        ["callbacks", "path", "expected_buffer"],
        [
            pytest.param(
                [],
                "/opt/airflow/dags/test_dag.py",
                b'{"file":"/opt/airflow/dags/test_dag.py","requests_fd":123,"callback_requests":[],'
                b'"type":"DagFileParseRequest"}\n',
            ),
            pytest.param(
                [
                    DagCallbackRequest(
                        full_filepath="/opt/airflow/dags/dag_callback_dag.py",
                        dag_id="dag_id",
                        run_id="run_id",
                        is_failure_callback=False,
                    )
                ],
                "/opt/airflow/dags/dag_callback_dag.py",
                b'{"file":"/opt/airflow/dags/dag_callback_dag.py","requests_fd":123,"callback_requests":'
                b'[{"full_filepath":"/opt/airflow/dags/dag_callback_dag.py","msg":null,"dag_id":"dag_id",'
                b'"run_id":"run_id","is_failure_callback":false,"type":"DagCallbackRequest"}],'
                b'"type":"DagFileParseRequest"}\n',
            ),
        ],
    )
    def test_serialize_callback_requests(self, callbacks, path, expected_buffer):
        processor = self.mock_processor()
        processor._on_child_started(callbacks, path)

        # Verify the response was added to the buffer
        val = processor.stdin.getvalue()
        assert val == expected_buffer

    @conf_vars({("core", "load_examples"): "False"})
    @pytest.mark.execution_timeout(10)
    def test_dag_with_system_exit(self, configure_testing_dag_bundle):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = "exit_test_dag"
        dag_directory = TEST_DAG_FOLDER.parent / "dags_with_system_exit"

        # Delete the one valid DAG/SerializedDAG, and check that it gets re-created
        clear_db_dags()
        clear_db_serialized_dags()

        with configure_testing_dag_bundle(dag_directory):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        # Three files in folder should be processed
        assert sum(stat.run_count for stat in manager._file_stats.values()) == 3

        with create_session() as session:
            assert session.get(DagModel, dag_id) is not None

    @conf_vars({("core", "load_examples"): "False"})
    @pytest.mark.execution_timeout(30)
    def test_pipe_full_deadlock(self, configure_testing_dag_bundle):
        dag_filepath = TEST_DAG_FOLDER / "test_scheduler_dags.py"

        child_pipe, parent_pipe = multiprocessing.Pipe()

        # Shrink the buffers to exacerbate the problem!
        for fd in (parent_pipe.fileno(),):
            sock = socket.socket(fileno=fd)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            sock.detach()

        exit_event = threading.Event()

        # To test this behaviour we need something that continually fills the
        # parent pipe's buffer (and keeps it full).
        def keep_pipe_full(pipe, exit_event):
            for n in itertools.count(1):
                if exit_event.is_set():
                    break

                req = DagCallbackRequest(
                    full_filepath=dag_filepath.as_posix(), dag_id="test_dag", run_id="run_id"
                )
                logger.info("Sending DagCallbackRequests %d", n)
                try:
                    pipe.send(req)
                except TypeError:
                    # This is actually the error you get when the parent pipe
                    # is closed! Nicely handled, eh?
                    break
                except OSError:
                    break
                logger.debug("   Sent %d DagCallbackRequests", n)

        thread = threading.Thread(target=keep_pipe_full, args=(parent_pipe, exit_event))

        with configure_testing_dag_bundle(dag_filepath):
            manager = DagFileProcessorManager(
                # A reasonable large number to ensure that we trigger the deadlock
                max_runs=100,
                processor_timeout=5,
                signal_conn=child_pipe,
                # Make it loop sub-processes quickly. Need to be non-zero to exercise the bug, else it finishes
                # too quickly
                file_process_interval=0.01,
            )

            try:
                thread.start()

                # If this completes without hanging, then the test is good!
                with mock.patch.object(
                    DagFileProcessorProcess,
                    "start",
                    side_effect=lambda *args, **kwargs: self.mock_processor(),
                ):
                    manager.run()
                exit_event.set()
            finally:
                logger.info("Closing pipes")
                parent_pipe.close()
                child_pipe.close()
                logger.info("Closed pipes")
                logger.info("Joining thread")
                thread.join(timeout=1.0)
                logger.info("Joined thread")

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.dag_processing.manager.Stats.timing")
    @pytest.mark.skip("AIP-66: stats are not implemented yet")
    def test_send_file_processing_statsd_timing(
        self, statsd_timing_mock, tmp_path, configure_testing_dag_bundle
    ):
        path_to_parse = tmp_path / "temp_dag.py"
        dag_code = textwrap.dedent(
            """
            from airflow import DAG
            dag = DAG(dag_id='temp_dag')
            """
        )
        path_to_parse.write_text(dag_code)

        with configure_testing_dag_bundle(tmp_path):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        last_runtime = manager._file_stats[os.fspath(path_to_parse)].last_duration
        statsd_timing_mock.assert_has_calls(
            [
                mock.call("dag_processing.last_duration.temp_dag", last_runtime),
                mock.call("dag_processing.last_duration", last_runtime, tags={"file_name": "temp_dag"}),
            ],
            any_order=True,
        )

    def test_refresh_dags_dir_doesnt_delete_zipped_dags(self, tmp_path, configure_testing_dag_bundle):
        """Test DagFileProcessorManager._refresh_dag_dir method"""
        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        zipped_dag_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
        dagbag.process_file(zipped_dag_path)
        dag = dagbag.get_dag("test_zip_dag")
        DAG.bulk_write_to_db("testing", None, [dag])
        SerializedDagModel.write_dag(dag)

        with configure_testing_dag_bundle(zipped_dag_path):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        # Assert dag not deleted in SDM
        assert SerializedDagModel.has_dag("test_zip_dag")
        # assert code not deleted
        assert DagCode.has_dag(dag.dag_id)
        # assert dag still active
        assert dag.get_is_active()

    def test_refresh_dags_dir_deactivates_deleted_zipped_dags(self, tmp_path, configure_testing_dag_bundle):
        """Test DagFileProcessorManager._refresh_dag_dir method"""
        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        zipped_dag_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
        dagbag.process_file(zipped_dag_path)
        dag = dagbag.get_dag("test_zip_dag")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)

        # TODO: this test feels a bit fragile - pointing at the zip directly causes the test to fail
        # TODO: jed look at this more closely - bagbad then process_file?!

        # Mock might_contain_dag to mimic deleting the python file from the zip
        with mock.patch("airflow.dag_processing.manager.might_contain_dag", return_value=False):
            with configure_testing_dag_bundle(TEST_DAGS_FOLDER):
                manager = DagFileProcessorManager(max_runs=1)
                manager.run()

        # Deleting the python file should not delete SDM for versioning sake
        assert SerializedDagModel.has_dag("test_zip_dag")
        # assert code not deleted for versioning sake
        assert DagCode.has_dag(dag.dag_id)
        # assert dagversion was not deleted
        assert DagVersion.get_latest_version(dag.dag_id)
        # assert dag deactivated
        assert not dag.get_is_active()

    @conf_vars(
        {
            ("core", "load_examples"): "False",
            ("scheduler", "standalone_dag_processor"): "True",
        }
    )
    def test_fetch_callbacks_from_database(self, tmp_path, configure_testing_dag_bundle):
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        callback1 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            run_id="123",
        )
        callback2 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            run_id="456",
        )

        with create_session() as session:
            session.add(DbCallbackRequest(callback=callback1, priority_weight=11))
            session.add(DbCallbackRequest(callback=callback2, priority_weight=10))

        with configure_testing_dag_bundle(tmp_path):
            manager = DagFileProcessorManager(max_runs=1, standalone_dag_processor=True)

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 0

    @conf_vars(
        {
            ("scheduler", "standalone_dag_processor"): "True",
            ("scheduler", "max_callbacks_per_loop"): "2",
            ("core", "load_examples"): "False",
        }
    )
    def test_fetch_callbacks_from_database_max_per_loop(self, tmp_path, configure_testing_dag_bundle):
        """Test DagFileProcessorManager._fetch_callbacks method"""
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        with create_session() as session:
            for i in range(5):
                callback = DagCallbackRequest(
                    dag_id="test_start_date_scheduling",
                    full_filepath=str(dag_filepath),
                    is_failure_callback=True,
                    run_id=str(i),
                )
                session.add(DbCallbackRequest(callback=callback, priority_weight=i))

        with configure_testing_dag_bundle(tmp_path):
            manager = DagFileProcessorManager(max_runs=1)

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 3

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 1

    @conf_vars(
        {
            ("scheduler", "standalone_dag_processor"): "False",
            ("core", "load_examples"): "False",
        }
    )
    def test_fetch_callbacks_from_database_not_standalone(self, tmp_path, configure_testing_dag_bundle):
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        with create_session() as session:
            callback = DagCallbackRequest(
                dag_id="test_start_date_scheduling",
                full_filepath=str(dag_filepath),
                is_failure_callback=True,
                run_id="123",
            )
            session.add(DbCallbackRequest(callback=callback, priority_weight=10))

        with configure_testing_dag_bundle(tmp_path):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        # Verify no callbacks removed from database.
        with create_session() as session:
            assert session.query(DbCallbackRequest).count() == 1

    @pytest.mark.skip("AIP-66: callbacks are not implemented yet")
    def test_callback_queue(self, tmp_path):
        # given
        manager = DagFileProcessorManager(
            max_runs=1,
            processor_timeout=365 * 86_400,
        )

        dag1_path = DagFileInfo(bundle_name="testing", path="/green_eggs/ham/file1.py")
        dag1_req1 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            run_id="run1",
            is_failure_callback=False,
            msg=None,
        )
        dag1_req2 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            run_id="run1",
            is_failure_callback=False,
            msg=None,
        )

        dag2_path = DagFileInfo(bundle_name="testing", path="/green_eggs/ham/file2.py")
        dag2_req1 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file2.py",
            dag_id="dag2",
            run_id="run1",
            is_failure_callback=False,
            msg=None,
        )

        # when
        manager._add_callback_to_queue(dag1_req1)
        manager._add_callback_to_queue(dag2_req1)

        # then - requests should be in manager's queue, with dag2 ahead of dag1 (because it was added last)
        assert manager._file_path_queue == deque([dag2_path, dag1_path])
        assert set(manager._callback_to_execute.keys()) == {
            dag1_req1.full_filepath,
            dag2_req1.full_filepath,
        }
        assert manager._callback_to_execute[dag2_req1.full_filepath] == [dag2_req1]

        # update the queue, although the callback is registered
        assert manager._file_path_queue == deque([dag2_req1.full_filepath, dag1_req1.full_filepath])

        # when
        manager._add_callback_to_queue(dag1_req2)

        # then - non-sla callback should have brought dag1 to the fore
        assert manager._file_path_queue == deque([dag1_req1.full_filepath, dag2_req1.full_filepath])
        assert manager._callback_to_execute[dag1_req1.full_filepath] == [
            dag1_req1,
            dag1_req2,
        ]

        with mock.patch.object(
            DagFileProcessorProcess, "start", side_effect=lambda *args, **kwargs: self.mock_processor()
        ) as start:
            manager._start_new_processes()
        # Callbacks passed to process ctor
        start.assert_any_call(
            id=mock.ANY, path=dag1_req1.full_filepath, callbacks=[dag1_req1, dag1_req2], selector=mock.ANY
        )
        # And removed from the queue
        assert dag1_req1.full_filepath not in manager._callback_to_execute

    def test_dag_with_assets(self, session, configure_testing_dag_bundle):
        """'Integration' test to ensure that the assets get parsed and stored correctly for parsed dags."""

        test_dag_path = str(TEST_DAG_FOLDER / "test_assets.py")

        with configure_testing_dag_bundle(test_dag_path):
            manager = DagFileProcessorManager(
                max_runs=1,
                processor_timeout=365 * 86_400,
            )
            manager.run()

        dag_model = session.get(DagModel, ("dag_with_skip_task"))
        assert dag_model.task_outlet_asset_references == [
            TaskOutletAssetReference(asset_id=mock.ANY, dag_id="dag_with_skip_task", task_id="skip_task")
        ]

    def test_bundles_are_refreshed(self):
        """
        Ensure bundles are refreshed by the manager, when necessary.

        - always refresh all bundles when starting the manager
        - refresh if the bundle hasn't been refreshed in the refresh_interval
        - when the latest_version in the db doesn't match the version this manager knows about
        """
        config = [
            {
                "name": "bundleone",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"local_folder": "/dev/null", "refresh_interval": 0},
            },
            {
                "name": "bundletwo",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"local_folder": "/dev/null", "refresh_interval": 300},
            },
        ]

        bundleone = MagicMock()
        bundleone.name = "bundleone"
        bundleone.refresh_interval = 0
        bundleone.get_current_version.return_value = None
        bundletwo = MagicMock()
        bundletwo.name = "bundletwo"
        bundletwo.refresh_interval = 300
        bundletwo.get_current_version.return_value = None

        with conf_vars({("dag_bundles", "backends"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch(
                "airflow.dag_processing.bundles.manager.DagBundlesManager"
            ) as mock_bundle_manager:
                mock_bundle_manager.return_value._bundle_config = {"bundleone": None, "bundletwo": None}
                mock_bundle_manager.return_value.get_all_dag_bundles.return_value = [bundleone, bundletwo]

                # We should refresh bundleone twice, but bundletwo only once - it has a long refresh_interval
                manager = DagFileProcessorManager(max_runs=2)
                manager.run()
                assert bundleone.refresh.call_count == 2
                bundletwo.refresh.assert_called_once()

                # Now, we should refresh both bundles, regardless of the refresh_interval
                # as we are starting up a fresh manager
                bundleone.reset_mock()
                bundletwo.reset_mock()
                manager = DagFileProcessorManager(max_runs=2)
                manager.run()
                assert bundleone.refresh.call_count == 2
                bundletwo.refresh.assert_called_once()

                # however, if the version doesn't match, we should still refresh
                bundletwo.reset_mock()

                def _update_bundletwo_version():
                    # We will update the bundle version in the db, so the next manager loop
                    # will believe another processor had seen a new version
                    with create_session() as session:
                        bundletwo_model = session.get(DagBundleModel, "bundletwo")
                        bundletwo_model.latest_version = "123"

                bundletwo.refresh.side_effect = _update_bundletwo_version
                manager = DagFileProcessorManager(max_runs=2)
                manager.run()
                assert bundletwo.refresh.call_count == 2

    def test_bundles_versions_are_stored(self):
        config = [
            {
                "name": "mybundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"local_folder": "/dev/null", "refresh_interval": 0},
            },
        ]

        mybundle = MagicMock()
        mybundle.name = "bundleone"
        mybundle.refresh_interval = 0
        mybundle.supports_versioning = True
        mybundle.get_current_version.return_value = "123"

        with conf_vars({("dag_bundles", "backends"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch(
                "airflow.dag_processing.bundles.manager.DagBundlesManager"
            ) as mock_bundle_manager:
                mock_bundle_manager.return_value._bundle_config = {"bundleone": None}
                mock_bundle_manager.return_value.get_all_dag_bundles.return_value = [mybundle]
                manager = DagFileProcessorManager(max_runs=1)
                manager.run()

        with create_session() as session:
            model = session.get(DagBundleModel, "bundleone")
            assert model.latest_version == "123"


class TestDagFileProcessorAgent:
    @pytest.fixture(autouse=True)
    def _disable_examples(self):
        with conf_vars({("core", "load_examples"): "False"}):
            yield

    def test_launch_process(self):
        from airflow.configuration import conf

        log_file_loc = conf.get("logging", "DAG_PROCESSOR_MANAGER_LOG_LOCATION")
        with suppress(OSError):
            os.remove(log_file_loc)

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(0, timedelta(days=365))
        processor_agent.start()

        processor_agent._process.join()

        assert os.path.isfile(log_file_loc)

    def test_get_callbacks_pipe(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        retval = processor_agent.get_callbacks_pipe()
        assert retval == processor_agent._parent_signal_conn

    def test_get_callbacks_pipe_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.get_callbacks_pipe()

    def test_heartbeat_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.heartbeat()

    def test_heartbeat_poll_eof_error(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.return_value = True
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.side_effect = EOFError
        ret_val = processor_agent.heartbeat()
        assert ret_val is None

    def test_heartbeat_poll_connection_error(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.return_value = True
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.side_effect = ConnectionError
        ret_val = processor_agent.heartbeat()
        assert ret_val is None

    def test_heartbeat_poll_process_message(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.side_effect = [True, False]
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.return_value = "testelem"
        with mock.patch.object(processor_agent, "_process_message"):
            processor_agent.heartbeat()
            processor_agent._process_message.assert_called_with("testelem")

    def test_process_message_invalid_type(self):
        message = "xyz"
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        with pytest.raises(RuntimeError, match="Unexpected message received of type str"):
            processor_agent._process_message(message)

    @mock.patch("airflow.utils.process_utils.reap_process_group")
    def test_heartbeat_manager_process_restart(self, mock_pg, monkeypatch):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process = MagicMock()
        monkeypatch.setattr(processor_agent._process, "pid", 1234)
        monkeypatch.setattr(processor_agent._process, "exitcode", 1)
        processor_agent.start = Mock()
        processor_agent._process.is_alive.return_value = False
        with mock.patch.object(processor_agent._process, "join"):
            processor_agent._heartbeat_manager()
            processor_agent.start.assert_called()
            mock_pg.assert_not_called()

    @mock.patch("airflow.dag_processing.manager.Stats")
    @mock.patch("time.monotonic")
    @mock.patch("airflow.dag_processing.manager.reap_process_group")
    def test_heartbeat_manager_process_reap(self, mock_pg, mock_time_monotonic, mock_stats):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process = Mock()
        processor_agent._process.pid = 12345
        processor_agent._process.is_alive.return_value = True
        processor_agent._done = False

        processor_agent.log.error = Mock()
        processor_agent._processor_timeout = Mock()
        processor_agent._processor_timeout.total_seconds.return_value = 500
        mock_time_monotonic.return_value = 1000
        processor_agent._last_parsing_stat_received_at = 100
        processor_agent.start = Mock()

        processor_agent._heartbeat_manager()
        mock_stats.incr.assert_called()
        mock_pg.assert_called()
        processor_agent.log.error.assert_called()
        processor_agent.start.assert_called()

    def test_heartbeat_manager_end_no_process(self):
        processor_agent = DagFileProcessorAgent(1, timedelta(days=365))
        processor_agent._process = Mock()
        processor_agent._process.__bool__ = Mock(return_value=False)
        processor_agent._process.side_effect = [None]
        processor_agent.log.warning = Mock()

        processor_agent.end()
        processor_agent.log.warning.assert_called_with("Ending without manager process.")
        processor_agent._process.join.assert_not_called()

    @pytest.mark.execution_timeout(5)
    def test_terminate(self, tmp_path, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(tmp_path):
            processor_agent = DagFileProcessorAgent(-1, timedelta(days=365))

            processor_agent.start()
            try:
                processor_agent.terminate()

                processor_agent._process.join(timeout=1)
                assert processor_agent._process.is_alive() is False
                assert processor_agent._process.exitcode == 0
            except Exception:
                reap_process_group(processor_agent._process.pid, logger=logger)
                raise

    @conf_vars({("logging", "dag_processor_manager_log_stdout"): "True"})
    def test_log_to_stdout(self, capfd):
        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(0, timedelta(days=365))
        processor_agent.start()

        processor_agent._process.join()

        # Capture the stdout and stderr
        out, _ = capfd.readouterr()
        assert "DAG File Processing Stats" in out

    @conf_vars({("logging", "dag_processor_manager_log_stdout"): "False"})
    def test_not_log_to_stdout(self, capfd):
        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(0, timedelta(days=365))
        processor_agent.start()

        processor_agent._process.join()

        # Capture the stdout and stderr
        out, _ = capfd.readouterr()
        assert "DAG File Processing Stats" not in out
