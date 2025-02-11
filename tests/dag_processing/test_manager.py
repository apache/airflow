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
import json
import logging
import os
import random
import re
import shutil
import signal
import textwrap
import time
from collections import deque
from datetime import datetime, timedelta
from logging.config import dictConfig
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine
from sqlalchemy import func, select
from uuid6 import uuid7

from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.manager import (
    DagFileInfo,
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
from airflow.utils.session import create_session

from tests.models import TEST_DAGS_FOLDER
from tests_common.test_utils.compat import ParseImportError
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_callbacks,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

logger = logging.getLogger(__name__)
TEST_DAG_FOLDER = Path(__file__).parents[1].resolve() / "dags"
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def _get_file_infos(files: list[str | Path]) -> list[DagFileInfo]:
    return [DagFileInfo(bundle_name="testing", bundle_path=TEST_DAGS_FOLDER, rel_path=Path(f)) for f in files]


def mock_get_mtime(file: Path):
    f = str(file)
    m = re.match(pattern=r".*ss=(.+?)\.\w+", string=f)
    if not m:
        raise ValueError(f"unexpected: {file}")
    match = m.group(1)
    if match == "<class 'FileNotFoundError'>":
        raise FileNotFoundError()
    try:
        return int(match)
    except Exception:
        raise ValueError(f"could not convert value {match} to int")


def encode_mtime_in_filename(val):
    from pathlib import PurePath

    out = []
    for fname, mtime in val:
        f = PurePath(PurePath(fname).name)
        addition = f"ss={str(mtime)}"
        out.append(f"{f.stem}-{addition}{f.suffix}")
    return out


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
        clear_db_dag_bundles()

    def teardown_class(self):
        clear_db_assets()
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_callbacks()
        clear_db_import_errors()
        clear_db_dag_bundles()

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

    def test_start_new_processes_with_same_filepath(self, configure_testing_dag_bundle):
        """
        Test that when a processor already exist with a filepath, a new processor won't be created
        with that filepath. The filepath will just be removed from the list.
        """
        with configure_testing_dag_bundle("/tmp"):
            manager = DagFileProcessorManager(max_runs=1)
            manager._dag_bundles = list(DagBundlesManager().get_all_dag_bundles())

        file_1 = DagFileInfo(bundle_name="testing", rel_path=Path("file_1.py"), bundle_path=TEST_DAGS_FOLDER)
        file_2 = DagFileInfo(bundle_name="testing", rel_path=Path("file_2.py"), bundle_path=TEST_DAGS_FOLDER)
        file_3 = DagFileInfo(bundle_name="testing", rel_path=Path("file_3.py"), bundle_path=TEST_DAGS_FOLDER)
        manager._file_queue = deque([file_1, file_2, file_3])

        # Mock that only one processor exists. This processor runs with 'file_1'
        manager._processors[file_1] = MagicMock()
        # Start New Processes
        with mock.patch.object(DagFileProcessorManager, "_create_process"):
            manager._start_new_processes()

        # Because of the config: '[dag_processor] parsing_processes = 2'
        # verify that only one extra process is created
        # and since a processor with 'file_1' already exists,
        # even though it is first in '_file_path_queue'
        # a new processor is created with 'file_2' and not 'file_1'.

        assert file_1 in manager._processors.keys()
        assert file_2 in manager._processors.keys()
        assert deque([file_3]) == manager._file_queue

    def test_handle_removed_files_when_processor_file_path_not_in_new_file_paths(self):
        """Ensure processors and file stats are removed when the file path is not in the new file paths"""
        manager = DagFileProcessorManager(max_runs=1)
        bundle_name = "testing"
        file = DagFileInfo(
            bundle_name=bundle_name, rel_path=Path("missing_file.txt"), bundle_path=TEST_DAGS_FOLDER
        )

        manager._processors[file] = MagicMock()
        manager._file_stats[file] = DagFileStat()

        manager.handle_removed_files({bundle_name: set()})
        assert manager._processors == {}
        assert file not in manager._file_stats

    def test_handle_removed_files_when_processor_file_path_is_present(self):
        """handle_removed_files should not purge files that are still present."""
        manager = DagFileProcessorManager(max_runs=1)
        bundle_name = "testing"
        file = DagFileInfo(bundle_name=bundle_name, rel_path=Path("abc.txt"), bundle_path=TEST_DAGS_FOLDER)
        mock_processor = MagicMock()

        manager._processors[file] = mock_processor

        manager.handle_removed_files(known_files={bundle_name: {file}})
        assert manager._processors == {file: mock_processor}

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "alphabetical"})
    def test_files_in_queue_sorted_alphabetically(self):
        """Test dag files are sorted alphabetically"""
        file_names = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]
        dag_files = _get_file_infos(file_names)
        ordered_dag_files = _get_file_infos(sorted(file_names))

        manager = DagFileProcessorManager(max_runs=1)
        known_files = {"some-bundle": set(dag_files)}
        assert manager._file_queue == deque()
        manager.prepare_file_queue(known_files=known_files)
        assert manager._file_queue == deque(ordered_dag_files)

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "random_seeded_by_host"})
    def test_files_sorted_random_seeded_by_host(self):
        """Test files are randomly sorted and seeded by host name"""

        f_infos = _get_file_infos(["file_3.py", "file_2.py", "file_4.py", "file_1.py"])
        known_files = {"anything": f_infos}
        manager = DagFileProcessorManager(max_runs=1)

        assert manager._file_queue == deque()
        manager.prepare_file_queue(known_files=known_files)  # using list over test for reproducibility
        random.Random(get_hostname()).shuffle(f_infos)
        expected = deque(f_infos)
        assert manager._file_queue == expected

        # Verify running it again produces same order
        manager._files = []
        manager.prepare_file_queue(known_files=known_files)
        assert manager._file_queue == expected

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime", new=mock_get_mtime)
    def test_files_sorted_by_modified_time(self):
        """Test files are sorted by modified time"""
        paths_with_mtime = [
            ("file_3.py", 3.0),
            ("file_2.py", 2.0),
            ("file_4.py", 5.0),
            ("file_1.py", 4.0),
        ]
        filenames = encode_mtime_in_filename(paths_with_mtime)
        dag_files = _get_file_infos(filenames)

        manager = DagFileProcessorManager(max_runs=1)

        assert manager._file_queue == deque()
        manager.prepare_file_queue(known_files={"any": set(dag_files)})
        ordered_files = _get_file_infos(
            [
                "file_4-ss=5.0.py",
                "file_1-ss=4.0.py",
                "file_3-ss=3.0.py",
                "file_2-ss=2.0.py",
            ]
        )
        assert manager._file_queue == deque(ordered_files)

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime", new=mock_get_mtime)
    def test_queued_files_exclude_missing_file(self):
        """Check that a file is not enqueued for processing if it has been deleted"""
        file_and_mtime = [("file_3.py", 2.0), ("file_2.py", 3.0), ("file_4.py", FileNotFoundError)]
        filenames = encode_mtime_in_filename(file_and_mtime)
        file_infos = _get_file_infos(filenames)
        manager = DagFileProcessorManager(max_runs=1)
        manager.prepare_file_queue(known_files={"any": set(file_infos)})
        ordered_files = _get_file_infos(["file_2-ss=3.0.py", "file_3-ss=2.0.py"])
        assert manager._file_queue == deque(ordered_files)

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime", new=mock_get_mtime)
    def test_add_new_file_to_parsing_queue(self):
        """Check that new file is added to parsing queue"""
        dag_files = _get_file_infos(["file_1-ss=2.0.py", "file_2-ss=3.0.py", "file_3-ss=4.0.py"])
        from random import Random

        Random("file_2.py").random()
        manager = DagFileProcessorManager(max_runs=1)

        manager.prepare_file_queue(known_files={"any": set(dag_files)})
        assert set(manager._file_queue) == set(dag_files)

        manager.prepare_file_queue(
            known_files={"any": set((*dag_files, *_get_file_infos(["file_4-ss=1.0.py"])))}
        )
        # manager.add_files_to_queue()
        ordered_files = _get_file_infos(
            [
                "file_3-ss=4.0.py",
                "file_2-ss=3.0.py",
                "file_1-ss=2.0.py",
                "file_4-ss=1.0.py",
            ]
        )
        assert manager._file_queue == deque(ordered_files)

    @conf_vars({("dag_processor", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_recently_modified_file_is_parsed_with_mtime_mode(self, mock_getmtime):
        """
        Test recently updated files are processed even if min_file_process_interval is not reached
        """
        freezed_base_time = timezone.datetime(2020, 1, 5, 0, 0, 0)
        initial_file_1_mtime = (freezed_base_time - timedelta(minutes=5)).timestamp()
        dag_file = DagFileInfo(
            bundle_name="testing", rel_path=Path("file_1.py"), bundle_path=TEST_DAGS_FOLDER
        )
        known_files = {"does-not-matter": {dag_file}}
        mock_getmtime.side_effect = [initial_file_1_mtime]

        manager = DagFileProcessorManager(max_runs=3)

        # let's say the DAG was just parsed 10 seconds before the Freezed time
        last_finish_time = freezed_base_time - timedelta(seconds=10)
        manager._file_stats = {
            dag_file: DagFileStat(1, 0, last_finish_time, 1.0, 1, 1),
        }
        with time_machine.travel(freezed_base_time):
            assert manager._file_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            manager.prepare_file_queue(known_files=known_files)
            assert manager._file_queue == deque()

        # Simulate the DAG modification by using modified_time which is greater
        # than the last_parse_time but still less than now - min_file_process_interval
        file_1_new_mtime = freezed_base_time - timedelta(seconds=5)
        file_1_new_mtime_ts = file_1_new_mtime.timestamp()
        with time_machine.travel(freezed_base_time):
            assert manager._file_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            mock_getmtime.side_effect = [file_1_new_mtime_ts]
            manager.prepare_file_queue(known_files=known_files)
            # Check that file is added to the queue even though file was just recently passed
            assert manager._file_queue == deque([dag_file])
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

        manager.handle_removed_files(dag_files)
        manager._file_queue = deque(["file_2.py", "file_3.py", "file_4.py", "file_1.py"])
        manager._refresh_requested_filelocs()
        assert manager._file_queue == deque(["file_1.py", "file_2.py", "file_3.py", "file_4.py"])
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
            rel_path=Path("test_example_bash_operator.py"),
            bundle_path=TEST_DAGS_FOLDER,
        )
        dagbag = DagBag(
            test_dag_path.absolute_path,
            read_dags_from_db=False,
            include_examples=False,
            bundle_path=test_dag_path.bundle_path,
        )

        with create_session() as session:
            # Add stale DAG to the DB
            dag = dagbag.get_dag("test_example_bash_operator")
            dag.last_parsed_time = timezone.utcnow()
            DAG.bulk_write_to_db("testing", None, [dag])
            SerializedDagModel.write_dag(dag, bundle_name="testing")

            # Add DAG to the file_parsing_stats
            stat = DagFileStat(
                num_dags=1,
                import_errors=0,
                last_finish_time=timezone.utcnow() + timedelta(hours=1),
                last_duration=1,
                run_count=1,
                last_num_of_db_queries=1,
            )
            manager._files = [test_dag_path]
            manager._file_stats[test_dag_path] = stat

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(
                    DagModel.is_active,
                    DagModel.relative_fileloc == str(test_dag_path.rel_path),
                    DagModel.bundle_name == test_dag_path.bundle_name,
                )
                .scalar()
            )
            assert active_dag_count == 1

            manager._scan_stale_dags()

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(
                    DagModel.is_active,
                    DagModel.relative_fileloc == str(test_dag_path.rel_path),
                    DagModel.bundle_name == test_dag_path.bundle_name,
                )
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
        manager._processors = {
            DagFileInfo(
                bundle_name="testing", rel_path=Path("abc.txt"), bundle_path=TEST_DAGS_FOLDER
            ): processor
        }
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
        manager._processors = {
            DagFileInfo(
                bundle_name="testing", rel_path=Path("abc.txt"), bundle_path=TEST_DAGS_FOLDER
            ): processor
        }
        with mock.patch.object(type(processor), "kill") as mock_kill:
            manager._kill_timed_out_processors()
        mock_kill.assert_not_called()

    @pytest.mark.usefixtures("testing_dag_bundle")
    @pytest.mark.parametrize(
        ["callbacks", "path", "expected_buffer"],
        [
            pytest.param(
                [],
                "/opt/airflow/dags/test_dag.py",
                b"{"
                b'"file":"/opt/airflow/dags/test_dag.py",'
                b'"bundle_path":"/opt/airflow/dags",'
                b'"requests_fd":123,'
                b'"callback_requests":[],'
                b'"type":"DagFileParseRequest"'
                b"}\n",
            ),
            pytest.param(
                [
                    DagCallbackRequest(
                        filepath="dag_callback_dag.py",
                        dag_id="dag_id",
                        run_id="run_id",
                        bundle_name="testing",
                        bundle_version=None,
                        is_failure_callback=False,
                    )
                ],
                "/opt/airflow/dags/dag_callback_dag.py",
                b"{"
                b'"file":"/opt/airflow/dags/dag_callback_dag.py",'
                b'"bundle_path":"/opt/airflow/dags",'
                b'"requests_fd":123,"callback_requests":'
                b"["
                b"{"
                b'"filepath":"dag_callback_dag.py",'
                b'"bundle_name":"testing",'
                b'"bundle_version":null,'
                b'"msg":null,'
                b'"dag_id":"dag_id",'
                b'"run_id":"run_id",'
                b'"is_failure_callback":false,'
                b'"type":"DagCallbackRequest"'
                b"}"
                b"],"
                b'"type":"DagFileParseRequest"'
                b"}\n",
            ),
        ],
    )
    def test_serialize_callback_requests(self, callbacks, path, expected_buffer):
        processor = self.mock_processor()
        processor._on_child_started(callbacks, path, bundle_path=Path("/opt/airflow/dags"))

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

    def test_refresh_dags_dir_doesnt_delete_zipped_dags(
        self, tmp_path, testing_dag_bundle, configure_testing_dag_bundle, test_zip_path
    ):
        """Test DagFileProcessorManager._refresh_dag_dir method"""
        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        dagbag.process_file(test_zip_path)
        dag = dagbag.get_dag("test_zip_dag")
        DAG.bulk_write_to_db("testing", None, [dag])
        SerializedDagModel.write_dag(dag, bundle_name="testing")

        with configure_testing_dag_bundle(test_zip_path):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

        # Assert dag not deleted in SDM
        assert SerializedDagModel.has_dag("test_zip_dag")
        # assert code not deleted
        assert DagCode.has_dag(dag.dag_id)
        # assert dag still active
        assert dag.get_is_active()

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_refresh_dags_dir_deactivates_deleted_zipped_dags(
        self, session, tmp_path, configure_testing_dag_bundle, test_zip_path
    ):
        """Test DagFileProcessorManager._refresh_dag_dir method"""
        dag_id = "test_zip_dag"
        filename = "test_zip.zip"
        source_location = test_zip_path
        bundle_path = Path(tmp_path, "test_refresh_dags_dir_deactivates_deleted_zipped_dags")
        bundle_path.mkdir(exist_ok=True)
        zip_dag_path = bundle_path / filename
        shutil.copy(source_location, zip_dag_path)

        with configure_testing_dag_bundle(bundle_path):
            manager = DagFileProcessorManager(max_runs=1)
            manager.run()

            assert SerializedDagModel.has_dag(dag_id)
            assert DagCode.has_dag(dag_id)
            assert DagVersion.get_latest_version(dag_id)
            dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
            assert dag.is_active is True

            os.remove(zip_dag_path)

            manager.run()

            assert SerializedDagModel.has_dag(dag_id)
            assert DagCode.has_dag(dag_id)
            assert DagVersion.get_latest_version(dag_id)
            dag = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))
            assert dag.is_active is False

    def test_deactivate_deleted_dags(self, dag_maker):
        with dag_maker("test_dag1") as dag1:
            dag1.relative_fileloc = "test_dag1.py"
        with dag_maker("test_dag2") as dag2:
            dag2.relative_fileloc = "test_dag2.py"
        dag_maker.sync_dagbag_to_db()

        active_files = [
            DagFileInfo(
                bundle_name="dag_maker",
                rel_path=Path("test_dag1.py"),
                bundle_path=TEST_DAGS_FOLDER,
            ),
            # Mimic that the test_dag2.py file is deleted
        ]

        manager = DagFileProcessorManager(max_runs=1)
        manager.deactivate_deleted_dags("dag_maker", active_files)

        dagbag = DagBag(read_dags_from_db=True)
        # The DAG from test_dag1.py is still active
        assert dagbag.get_dag("test_dag1").get_is_active() is True
        # and the DAG from test_dag2.py is deactivated
        assert dagbag.get_dag("test_dag2").get_is_active() is False

    @conf_vars({("core", "load_examples"): "False"})
    def test_fetch_callbacks_from_database(self, configure_testing_dag_bundle):
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        callback1 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            bundle_name="testing",
            bundle_version=None,
            filepath="test_on_failure_callback_dag.py",
            is_failure_callback=True,
            run_id="123",
        )
        callback2 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            bundle_name="testing",
            bundle_version=None,
            filepath="test_on_failure_callback_dag.py",
            is_failure_callback=True,
            run_id="456",
        )

        with create_session() as session:
            session.add(DbCallbackRequest(callback=callback1, priority_weight=11))
            session.add(DbCallbackRequest(callback=callback2, priority_weight=10))

        with configure_testing_dag_bundle(dag_filepath):
            manager = DagFileProcessorManager(max_runs=1)

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 0

    @conf_vars(
        {
            ("dag_processor", "max_callbacks_per_loop"): "2",
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
                    bundle_name="testing",
                    bundle_version=None,
                    filepath="test_on_failure_callback_dag.py",
                    is_failure_callback=True,
                    run_id=str(i),
                )
                session.add(DbCallbackRequest(callback=callback, priority_weight=i))

        with configure_testing_dag_bundle(dag_filepath):
            manager = DagFileProcessorManager(max_runs=1)

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 3

            with create_session() as session:
                manager.run()
                assert session.query(DbCallbackRequest).count() == 1

    @mock.patch.object(DagFileProcessorManager, "_get_logger_for_dag_file")
    def test_callback_queue(self, mock_logger, configure_testing_dag_bundle):
        tmp_path = "/green_eggs/ham"
        with configure_testing_dag_bundle(tmp_path):
            # given
            manager = DagFileProcessorManager(
                max_runs=1,
                processor_timeout=365 * 86_400,
            )
            manager._dag_bundles = list(DagBundlesManager().get_all_dag_bundles())

            dag1_path = DagFileInfo(
                bundle_name="testing", rel_path=Path("file1.py"), bundle_path=Path(tmp_path)
            )
            dag1_req1 = DagCallbackRequest(
                filepath="file1.py",
                dag_id="dag1",
                run_id="run1",
                is_failure_callback=False,
                bundle_name="testing",
                bundle_version=None,
                msg=None,
            )
            dag1_req2 = DagCallbackRequest(
                filepath="file1.py",
                dag_id="dag1",
                run_id="run1",
                is_failure_callback=False,
                bundle_name="testing",
                bundle_version=None,
                msg=None,
            )

            dag2_path = DagFileInfo(
                bundle_name="testing", rel_path=Path("file2.py"), bundle_path=Path(tmp_path)
            )
            dag2_req1 = DagCallbackRequest(
                filepath="file2.py",
                dag_id="dag2",
                run_id="run1",
                bundle_name=dag2_path.bundle_name,
                bundle_version=None,
                is_failure_callback=False,
                msg=None,
            )

            # when
            manager._add_callback_to_queue(dag1_req1)
            manager._add_callback_to_queue(dag2_req1)

            # then - requests should be in manager's queue, with dag2 ahead of dag1 (because it was added last)
            assert manager._file_queue == deque([dag2_path, dag1_path])
            assert set(manager._callback_to_execute.keys()) == {
                dag1_path,
                dag2_path,
            }
            assert manager._callback_to_execute[dag2_path] == [dag2_req1]

            # update the queue, although the callback is registered
            assert manager._file_queue == deque([dag2_path, dag1_path])

            # when
            manager._add_callback_to_queue(dag1_req2)
            # Since dag1_req2 is same as dag1_req1, we now have 2 items in file_path_queue
            assert manager._file_queue == deque([dag2_path, dag1_path])
            assert manager._callback_to_execute[dag1_path] == [
                dag1_req1,
                dag1_req2,
            ]

            with mock.patch.object(
                DagFileProcessorProcess, "start", side_effect=lambda *args, **kwargs: self.mock_processor()
            ) as start:
                manager._start_new_processes()
            # Callbacks passed to processor
            assert start.call_args_list == [
                mock.call(
                    id=mock.ANY,
                    path=Path(dag2_path.bundle_path, dag2_path.rel_path),
                    bundle_path=dag2_path.bundle_path,
                    callbacks=[dag2_req1],
                    selector=mock.ANY,
                    logger=mock_logger.return_value,
                ),
                mock.call(
                    id=mock.ANY,
                    path=Path(dag1_path.bundle_path, dag1_path.rel_path),
                    bundle_path=dag1_path.bundle_path,
                    callbacks=[dag1_req1, dag1_req2],
                    selector=mock.ANY,
                    logger=mock_logger.return_value,
                ),
            ]
            # And removed from the queue
            assert dag1_path not in manager._callback_to_execute
            assert dag2_path not in manager._callback_to_execute

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
                "kwargs": {"path": "/dev/null", "refresh_interval": 0},
            },
            {
                "name": "bundletwo",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": "/dev/null", "refresh_interval": 300},
            },
        ]

        bundleone = MagicMock()
        bundleone.name = "bundleone"
        bundleone.path = "/dev/null"
        bundleone.refresh_interval = 0
        bundleone.get_current_version.return_value = None
        bundletwo = MagicMock()
        bundletwo.name = "bundletwo"
        bundletwo.path = "/dev/null"
        bundletwo.refresh_interval = 300
        bundletwo.get_current_version.return_value = None

        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch("airflow.dag_processing.manager.DagBundlesManager") as mock_bundle_manager:
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
                        bundletwo_model.version = "123"

                bundletwo.refresh.side_effect = _update_bundletwo_version
                manager = DagFileProcessorManager(max_runs=2)
                manager.run()
                assert bundletwo.refresh.call_count == 2

    def test_bundles_versions_are_stored(self, session):
        config = [
            {
                "name": "bundleone",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": "/dev/null", "refresh_interval": 0},
            },
        ]

        mybundle = MagicMock()
        mybundle.name = "bundleone"
        mybundle.path = "/dev/null"
        mybundle.refresh_interval = 0
        mybundle.supports_versioning = True
        mybundle.get_current_version.return_value = "123"

        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch("airflow.dag_processing.manager.DagBundlesManager") as mock_bundle_manager:
                mock_bundle_manager.return_value._bundle_config = {"bundleone": None}
                mock_bundle_manager.return_value.get_all_dag_bundles.return_value = [mybundle]
                manager = DagFileProcessorManager(max_runs=1)
                manager.run()

        with create_session() as session:
            model = session.get(DagBundleModel, "bundleone")
            assert model.version == "123"

    def test_non_versioned_bundle_get_version_not_called(self):
        config = [
            {
                "name": "bundleone",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": "/dev/null", "refresh_interval": 0},
            },
        ]

        bundleone = MagicMock()
        bundleone.name = "bundleone"
        bundleone.refresh_interval = 0
        bundleone.supports_versioning = False
        bundleone.path = Path("/dev/null")

        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch("airflow.dag_processing.manager.DagBundlesManager") as mock_bundle_manager:
                mock_bundle_manager.return_value._bundle_config = {"bundleone": None}
                mock_bundle_manager.return_value.get_all_dag_bundles.return_value = [bundleone]
                manager = DagFileProcessorManager(max_runs=1)
                manager.run()

        bundleone.refresh.assert_called_once()
        bundleone.get_current_version.assert_not_called()

    def test_versioned_bundle_get_version_called_once(self):
        """Make sure in a normal "warm" loop, get_current_version is called just once after refresha"""

        config = [
            {
                "name": "bundleone",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": "/dev/null", "refresh_interval": 0},
            },
        ]

        bundleone = MagicMock()
        bundleone.name = "bundleone"
        bundleone.refresh_interval = 0
        bundleone.supports_versioning = True
        bundleone.get_current_version.return_value = "123"
        bundleone.path = Path("/dev/null")

        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(config)}):
            DagBundlesManager().sync_bundles_to_db()
            with mock.patch("airflow.dag_processing.manager.DagBundlesManager") as mock_bundle_manager:
                mock_bundle_manager.return_value._bundle_config = {"bundleone": None}
                mock_bundle_manager.return_value.get_all_dag_bundles.return_value = [bundleone]
                manager = DagFileProcessorManager(max_runs=1)
                manager.run()  # run it once to warm up

                # now run it again so we can check we only call get_current_version once
                bundleone.refresh.reset_mock()
                bundleone.get_current_version.reset_mock()
                manager.run()
                bundleone.refresh.assert_called_once()
                bundleone.get_current_version.assert_called_once()

    @pytest.mark.parametrize(
        "bundle_names, expected",
        [
            (None, {"bundle1", "bundle2", "bundle3"}),
            (["bundle1"], {"bundle1"}),
            (["bundle1", "bundle2"], {"bundle1", "bundle2"}),
        ],
    )
    def test_bundle_names_to_parse(self, bundle_names, expected, configure_dag_bundles):
        config = {f"bundle{i}": os.devnull for i in range(1, 4)}
        with configure_dag_bundles(config):
            manager = DagFileProcessorManager(max_runs=1, bundle_names_to_parse=bundle_names)
            manager._run_parsing_loop = MagicMock()
            manager.run()

        bundle_names_being_parsed = {b.name for b in manager._dag_bundles}
        assert bundle_names_being_parsed == expected
