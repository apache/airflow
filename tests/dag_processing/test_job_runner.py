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

import contextlib
import itertools
import logging
import multiprocessing
import os
import pathlib
import random
import socket
import sys
import textwrap
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from logging.config import dictConfig
from unittest import mock
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest
import time_machine
from sqlalchemy import func

from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    SlaCallbackRequest,
    ToggleCallbackRequest,
)
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.configuration import conf
from airflow.dag_processing.manager import (
    DagFileProcessorAgent,
    DagFileProcessorManager,
    DagFileStat,
    DagParsingSignal,
    DagParsingStat,
)
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job
from airflow.models import DagBag, DagModel, DbCallbackRequest
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from tests.core.test_logging_config import SETTINGS_FILE_VALID, settings_context
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils.compat import ParseImportError
from tests.test_utils.config import conf_vars
from tests.test_utils.db import (
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


class FakeDagFileProcessorRunner(DagFileProcessorProcess):
    # This fake processor will return the zombies it received in constructor
    # as its processing result w/o actually parsing anything.
    def __init__(self, file_path, pickle_dags, dag_ids, dag_directory, callbacks):
        super().__init__(file_path, pickle_dags, dag_ids, dag_directory, callbacks)
        # We need a "real" selectable handle for waitable_handle to work
        readable, writable = multiprocessing.Pipe(duplex=False)
        writable.send("abc")
        writable.close()
        self._waitable_handle = readable
        self._result = 0, 0, 0

    def start(self):
        pass

    @property
    def start_time(self):
        return DEFAULT_DATE

    @property
    def pid(self):
        return 1234

    @property
    def done(self):
        return True

    @property
    def result(self):
        return self._result

    @staticmethod
    def _create_process(file_path, callback_requests, dag_ids, dag_directory, pickle_dags):
        return FakeDagFileProcessorRunner(
            file_path,
            pickle_dags,
            dag_ids,
            dag_directory,
            callback_requests,
        )

    @property
    def waitable_handle(self):
        return self._waitable_handle


class TestDagProcessorJobRunner:
    def setup_method(self):
        dictConfig(DEFAULT_LOGGING_CONFIG)
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_callbacks()

    def teardown_class(self):
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_callbacks()

    def run_processor_manager_one_loop(self, manager, parent_pipe):
        if not manager.processor._async_mode:
            parent_pipe.send(DagParsingSignal.AGENT_RUN_ONCE)

        results = []

        while True:
            manager.processor._run_parsing_loop()

            while parent_pipe.poll(timeout=0.01):
                obj = parent_pipe.recv()
                if not isinstance(obj, DagParsingStat):
                    results.append(obj)
                elif obj.done:
                    return results
            raise RuntimeError("Shouldn't get here - nothing to read, but manager not finished!")

    @pytest.fixture
    def clear_parse_import_errors(self):
        clear_db_import_errors()

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.usefixtures("clear_parse_import_errors")
    @conf_vars({("core", "load_examples"): "False"})
    def test_remove_file_clears_import_error(self, tmp_path):
        path_to_parse = tmp_path / "temp_dag.py"

        # Generate original import error
        path_to_parse.write_text("an invalid airflow DAG")

        child_pipe, parent_pipe = multiprocessing.Pipe()

        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=path_to_parse.parent,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=async_mode,
            ),
        )

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)

            import_errors = session.query(ParseImportError).all()
            assert len(import_errors) == 1

            path_to_parse.unlink()

            # Rerun the scheduler once the dag file has been removed
            self.run_processor_manager_one_loop(manager, parent_pipe)
            import_errors = session.query(ParseImportError).all()

            assert len(import_errors) == 0
            session.rollback()

        child_pipe.close()
        parent_pipe.close()

    @conf_vars({("core", "load_examples"): "False"})
    def test_max_runs_when_no_files(self, tmp_path):
        child_pipe, parent_pipe = multiprocessing.Pipe()

        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=os.fspath(tmp_path),
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=async_mode,
            ),
        )

        self.run_processor_manager_one_loop(manager, parent_pipe)
        child_pipe.close()
        parent_pipe.close()

    @pytest.mark.backend("mysql", "postgres")
    @mock.patch("airflow.dag_processing.processor.iter_airflow_imports")
    def test_start_new_processes_with_same_filepath(self, _):
        """
        Test that when a processor already exist with a filepath, a new processor won't be created
        with that filepath. The filepath will just be removed from the list.
        """
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        file_1 = "file_1.py"
        file_2 = "file_2.py"
        file_3 = "file_3.py"
        manager.processor._file_path_queue = deque([file_1, file_2, file_3])

        # Mock that only one processor exists. This processor runs with 'file_1'
        manager.processor._processors[file_1] = MagicMock()
        # Start New Processes
        manager.processor.start_new_processes()

        # Because of the config: '[scheduler] parsing_processes = 2'
        # verify that only one extra process is created
        # and since a processor with 'file_1' already exists,
        # even though it is first in '_file_path_queue'
        # a new processor is created with 'file_2' and not 'file_1'.

        assert file_1 in manager.processor._processors.keys()
        assert file_2 in manager.processor._processors.keys()
        assert deque([file_3]) == manager.processor._file_path_queue

    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError("DagFileProcessor object has no attribute stop")
        mock_processor.terminate.side_effect = None

        manager.processor._processors["missing_file.txt"] = mock_processor
        manager.processor._file_stats["missing_file.txt"] = DagFileStat(0, 0, None, None, 0, 0)

        manager.processor.set_file_paths(["abc.txt"])
        assert manager.processor._processors == {}
        assert "missing_file.txt" not in manager.processor._file_stats

    def test_set_file_paths_when_processor_file_path_is_in_new_file_paths(self):
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError("DagFileProcessor object has no attribute stop")
        mock_processor.terminate.side_effect = None

        manager.processor._processors["abc.txt"] = mock_processor

        manager.processor.set_file_paths(["abc.txt"])
        assert manager.processor._processors == {"abc.txt": mock_processor}

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "alphabetical"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    def test_file_paths_in_queue_sorted_alphabetically(
        self, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        """Test dag files are sorted alphabetically"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        assert manager.processor._file_path_queue == deque()
        manager.processor.prepare_file_path_queue()
        assert manager.processor._file_path_queue == deque(
            ["file_1.py", "file_2.py", "file_3.py", "file_4.py"]
        )

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "random_seeded_by_host"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    def test_file_paths_in_queue_sorted_random_seeded_by_host(
        self, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        """Test files are randomly sorted and seeded by host name"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        assert manager.processor._file_path_queue == deque()
        manager.processor.prepare_file_path_queue()

        expected_order = deque(dag_files)
        random.Random(get_hostname()).shuffle(expected_order)
        assert manager.processor._file_path_queue == expected_order

        # Verify running it again produces same order
        manager.processor._file_paths = []
        manager.processor.prepare_file_path_queue()
        assert manager.processor._file_path_queue == expected_order

    @pytest.fixture
    def change_platform_timezone(self, monkeypatch):
        monkeypatch.setenv("TZ", "Europe/Paris")

        # propagate new timezone to C routines
        # this is only needed for Unix. On Windows, exporting the TZ env variable
        # is enough (see https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/localtime-s-localtime32-s-localtime64-s?view=msvc-170#remarks)
        tzset = getattr(time, "tzset", None)
        if tzset is not None:
            tzset()

        yield

        # reset timezone to platform's default
        monkeypatch.delenv("TZ")
        if tzset is not None:
            tzset()

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_sorted_by_modified_time(
        self,
        mock_getmtime,
        mock_isfile,
        mock_find_path,
        mock_might_contain_dag,
        mock_zipfile,
        change_platform_timezone,
    ):
        """Test files are sorted by modified time"""
        paths_with_mtime = {"file_3.py": 3.0, "file_2.py": 2.0, "file_4.py": 5.0, "file_1.py": 4.0}
        dag_files = list(paths_with_mtime.keys())
        mock_getmtime.side_effect = list(paths_with_mtime.values())
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        assert manager.processor._file_path_queue == deque()
        manager.processor.prepare_file_path_queue()
        assert manager.processor._file_path_queue == deque(
            ["file_4.py", "file_1.py", "file_3.py", "file_2.py"]
        )

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_excludes_missing_file(
        self,
        mock_getmtime,
        mock_isfile,
        mock_find_path,
        mock_might_contain_dag,
        mock_zipfile,
        change_platform_timezone,
    ):
        """Check that a file is not enqueued for processing if it has been deleted"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py"]
        mock_getmtime.side_effect = [1.0, 2.0, FileNotFoundError()]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        manager.processor.prepare_file_path_queue()
        assert manager.processor._file_path_queue == deque(["file_2.py", "file_3.py"])

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_add_new_file_to_parsing_queue(
        self,
        mock_getmtime,
        mock_isfile,
        mock_find_path,
        mock_might_contain_dag,
        mock_zipfile,
        change_platform_timezone,
    ):
        """Check that new file is added to parsing queue"""
        dag_files = ["file_1.py", "file_2.py", "file_3.py"]
        mock_getmtime.side_effect = [1.0, 2.0, 3.0]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        manager.processor.prepare_file_path_queue()
        assert manager.processor._file_path_queue == deque(["file_3.py", "file_2.py", "file_1.py"])

        manager.processor.set_file_paths([*dag_files, "file_4.py"])
        manager.processor.add_new_file_path_to_queue()
        assert manager.processor._file_path_queue == deque(
            ["file_4.py", "file_3.py", "file_2.py", "file_1.py"]
        )

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("airflow.settings.TIMEZONE", timezone.utc)
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_recently_modified_file_is_parsed_with_mtime_mode(
        self,
        mock_getmtime,
        mock_isfile,
        mock_find_path,
        mock_might_contain_dag,
        mock_zipfile,
        change_platform_timezone,
    ):
        """
        Test recently updated files are processed even if min_file_process_interval is not reached
        """
        freezed_base_time = timezone.datetime(2020, 1, 5, 0, 0, 0)
        initial_file_1_mtime = (freezed_base_time - timedelta(minutes=5)).timestamp()
        dag_files = ["file_1.py"]
        mock_getmtime.side_effect = [initial_file_1_mtime]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=3,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        # let's say the DAG was just parsed 10 seconds before the Freezed time
        last_finish_time = freezed_base_time - timedelta(seconds=10)
        manager.processor._file_stats = {
            "file_1.py": DagFileStat(1, 0, last_finish_time, timedelta(seconds=1.0), 1, 1),
        }
        with time_machine.travel(freezed_base_time):
            manager.processor.set_file_paths(dag_files)
            assert manager.processor._file_path_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            manager.processor.prepare_file_path_queue()
            assert manager.processor._file_path_queue == deque()

        # Simulate the DAG modification by using modified_time which is greater
        # than the last_parse_time but still less than now - min_file_process_interval
        file_1_new_mtime = freezed_base_time - timedelta(seconds=5)
        file_1_new_mtime_ts = file_1_new_mtime.timestamp()
        with time_machine.travel(freezed_base_time):
            manager.processor.set_file_paths(dag_files)
            assert manager.processor._file_path_queue == deque()
            # File Path Queue will be empty as the "modified time" < "last finish time"
            mock_getmtime.side_effect = [file_1_new_mtime_ts]
            manager.processor.prepare_file_path_queue()
            # Check that file is added to the queue even though file was just recently passed
            assert manager.processor._file_path_queue == deque(["file_1.py"])
            assert last_finish_time < file_1_new_mtime
            assert (
                manager.processor._file_process_interval
                > (freezed_base_time - manager.processor.get_last_finish_time("file_1.py")).total_seconds()
            )

    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    def test_file_paths_in_queue_sorted_by_priority(
        self, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        from airflow.models.dagbag import DagPriorityParsingRequest

        parsing_request = DagPriorityParsingRequest(fileloc="file_1.py")
        with create_session() as session:
            session.add(parsing_request)
            session.commit()

        """Test dag files are sorted by priority"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py", "file_1.py"]
        mock_find_path.return_value = dag_files

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor.set_file_paths(dag_files)
        manager.processor._file_path_queue = deque(["file_2.py", "file_3.py", "file_4.py", "file_1.py"])
        manager.processor._refresh_requested_filelocs()
        assert manager.processor._file_path_queue == deque(
            ["file_1.py", "file_2.py", "file_3.py", "file_4.py"]
        )
        with create_session() as session2:
            parsing_request_after = session2.query(DagPriorityParsingRequest).get(parsing_request.id)
        assert parsing_request_after is None

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_scan_stale_dags(self):
        """
        Ensure that DAGs are marked inactive when the file is parsed but the
        DagModel.last_parsed_time is not updated.
        """
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(minutes=10),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        test_dag_path = str(TEST_DAG_FOLDER / "test_example_bash_operator.py")
        dagbag = DagBag(test_dag_path, read_dags_from_db=False, include_examples=False)

        with create_session() as session:
            # Add stale DAG to the DB
            dag = dagbag.get_dag("test_example_bash_operator")
            dag.last_parsed_time = timezone.utcnow()
            dag.sync_to_db()
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
            manager.processor._file_paths = [test_dag_path]
            manager.processor._file_stats[test_dag_path] = stat

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(DagModel.is_active, DagModel.fileloc == test_dag_path)
                .scalar()
            )
            assert active_dag_count == 1

            serialized_dag_count = (
                session.query(func.count(SerializedDagModel.dag_id))
                .filter(SerializedDagModel.fileloc == test_dag_path)
                .scalar()
            )
            assert serialized_dag_count == 1

            manager.processor._scan_stale_dags()

            active_dag_count = (
                session.query(func.count(DagModel.dag_id))
                .filter(DagModel.is_active, DagModel.fileloc == test_dag_path)
                .scalar()
            )
            assert active_dag_count == 0

            serialized_dag_count = (
                session.query(func.count(SerializedDagModel.dag_id))
                .filter(SerializedDagModel.fileloc == test_dag_path)
                .scalar()
            )
            assert serialized_dag_count == 0

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @conf_vars(
        {
            ("core", "load_examples"): "False",
            ("scheduler", "standalone_dag_processor"): "True",
            ("scheduler", "stale_dag_threshold"): "50",
        }
    )
    def test_scan_stale_dags_standalone_mode(self):
        """
        Ensure only dags from current dag_directory are updated
        """
        dag_directory = "directory"
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=dag_directory,
                max_runs=1,
                processor_timeout=timedelta(minutes=10),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        test_dag_path = str(TEST_DAG_FOLDER / "test_example_bash_operator.py")
        dagbag = DagBag(test_dag_path, read_dags_from_db=False)
        other_test_dag_path = str(TEST_DAG_FOLDER / "test_scheduler_dags.py")
        other_dagbag = DagBag(other_test_dag_path, read_dags_from_db=False)

        with create_session() as session:
            # Add stale DAG to the DB
            dag = dagbag.get_dag("test_example_bash_operator")
            dag.last_parsed_time = timezone.utcnow()
            dag.sync_to_db(processor_subdir=dag_directory)

            # Add stale DAG to the DB
            other_dag = other_dagbag.get_dag("test_start_date_scheduling")
            other_dag.last_parsed_time = timezone.utcnow()
            other_dag.sync_to_db(processor_subdir="other")

            # Add DAG to the file_parsing_stats
            stat = DagFileStat(
                num_dags=1,
                import_errors=0,
                last_finish_time=timezone.utcnow() + timedelta(hours=1),
                last_duration=1,
                run_count=1,
                last_num_of_db_queries=1,
            )
            manager.processor._file_paths = [test_dag_path]
            manager.processor._file_stats[test_dag_path] = stat

            active_dag_count = session.query(func.count(DagModel.dag_id)).filter(DagModel.is_active).scalar()
            assert active_dag_count == 2

            manager.processor._scan_stale_dags()

            active_dag_count = session.query(func.count(DagModel.dag_id)).filter(DagModel.is_active).scalar()
            assert active_dag_count == 1

    @mock.patch(
        "airflow.dag_processing.processor.DagFileProcessorProcess.waitable_handle", new_callable=PropertyMock
    )
    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.pid", new_callable=PropertyMock)
    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.kill")
    def test_kill_timed_out_processors_kill(self, mock_kill, mock_pid, mock_waitable_handle):
        mock_pid.return_value = 1234
        mock_waitable_handle.return_value = 3
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory="directory",
                max_runs=1,
                processor_timeout=timedelta(seconds=5),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        processor = DagFileProcessorProcess(
            file_path="abc.txt",
            pickle_dags=False,
            dag_ids=[],
            dag_directory=TEST_DAG_FOLDER,
            callback_requests=[],
        )
        processor._start_time = timezone.make_aware(datetime.min)
        manager.processor._processors = {"abc.txt": processor}
        manager.processor.waitables[3] = processor
        initial_waitables = len(manager.processor.waitables)
        manager.processor._kill_timed_out_processors()
        mock_kill.assert_called_once_with()
        assert len(manager.processor._processors) == 0
        assert len(manager.processor.waitables) == initial_waitables - 1

    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.pid", new_callable=PropertyMock)
    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess")
    def test_kill_timed_out_processors_no_kill(self, mock_dag_file_processor, mock_pid):
        mock_pid.return_value = 1234
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=TEST_DAG_FOLDER,
                max_runs=1,
                processor_timeout=timedelta(seconds=5),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        processor = DagFileProcessorProcess(
            file_path="abc.txt",
            pickle_dags=False,
            dag_ids=[],
            dag_directory=str(TEST_DAG_FOLDER),
            callback_requests=[],
        )
        processor._start_time = timezone.make_aware(datetime.max)
        manager.processor._processors = {"abc.txt": processor}
        manager.processor._kill_timed_out_processors()
        mock_dag_file_processor.kill.assert_not_called()

    @conf_vars({("core", "load_examples"): "False"})
    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.execution_timeout(10)
    def test_dag_with_system_exit(self):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = "exit_test_dag"
        dag_directory = TEST_DAG_FOLDER.parent / "dags_with_system_exit"

        # Delete the one valid DAG/SerializedDAG, and check that it gets re-created
        clear_db_dags()
        clear_db_serialized_dags()

        child_pipe, parent_pipe = multiprocessing.Pipe()

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=dag_directory,
                dag_ids=[],
                max_runs=1,
                processor_timeout=timedelta(seconds=5),
                signal_conn=child_pipe,
                pickle_dags=False,
                async_mode=True,
            ),
        )

        manager.processor._run_parsing_loop()

        result = None
        while parent_pipe.poll(timeout=None):
            result = parent_pipe.recv()
            if isinstance(result, DagParsingStat) and result.done:
                break

        # Three files in folder should be processed
        assert sum(stat.run_count for stat in manager.processor._file_stats.values()) == 3

        with create_session() as session:
            assert session.get(DagModel, dag_id) is not None

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @conf_vars({("core", "load_examples"): "False"})
    def test_import_error_with_dag_directory(self, tmp_path):
        TEMP_DAG_FILENAME = "temp_dag.py"

        processor_dir_1 = tmp_path / "processor_1"
        processor_dir_1.mkdir()
        filename_1 = os.path.join(processor_dir_1, TEMP_DAG_FILENAME)
        with open(filename_1, "w") as f:
            f.write("an invalid airflow DAG")

        processor_dir_2 = tmp_path / "processor_2"
        processor_dir_2.mkdir()
        filename_1 = os.path.join(processor_dir_2, TEMP_DAG_FILENAME)
        with open(filename_1, "w") as f:
            f.write("an invalid airflow DAG")

        with create_session() as session:
            child_pipe, parent_pipe = multiprocessing.Pipe()

            manager = DagProcessorJobRunner(
                job=Job(),
                processor=DagFileProcessorManager(
                    dag_directory=processor_dir_1,
                    dag_ids=[],
                    max_runs=1,
                    signal_conn=child_pipe,
                    processor_timeout=timedelta(seconds=5),
                    pickle_dags=False,
                    async_mode=False,
                ),
            )

            self.run_processor_manager_one_loop(manager, parent_pipe)

            import_errors = session.query(ParseImportError).order_by("id").all()
            assert len(import_errors) == 1
            assert import_errors[0].processor_subdir == str(processor_dir_1)

            child_pipe, parent_pipe = multiprocessing.Pipe()

            manager = DagProcessorJobRunner(
                job=Job(),
                processor=DagFileProcessorManager(
                    dag_directory=processor_dir_2,
                    dag_ids=[],
                    max_runs=1,
                    signal_conn=child_pipe,
                    processor_timeout=timedelta(seconds=5),
                    pickle_dags=False,
                    async_mode=True,
                ),
            )

            self.run_processor_manager_one_loop(manager, parent_pipe)

            import_errors = session.query(ParseImportError).order_by("id").all()
            assert len(import_errors) == 2
            assert import_errors[0].processor_subdir == str(processor_dir_1)
            assert import_errors[1].processor_subdir == str(processor_dir_2)

            session.rollback()

    @conf_vars({("core", "load_examples"): "False"})
    @pytest.mark.backend("mysql", "postgres")
    @pytest.mark.execution_timeout(30)
    @mock.patch("airflow.dag_processing.manager.DagFileProcessorProcess")
    def test_pipe_full_deadlock(self, mock_processor):
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

                req = CallbackRequest(str(dag_filepath))
                logger.info("Sending CallbackRequests %d", n)
                try:
                    pipe.send(req)
                except TypeError:
                    # This is actually the error you get when the parent pipe
                    # is closed! Nicely handled, eh?
                    break
                except OSError:
                    break
                logger.debug("   Sent %d CallbackRequests", n)

        thread = threading.Thread(target=keep_pipe_full, args=(parent_pipe, exit_event))

        fake_processors = []

        def fake_processor_(*args, **kwargs):
            nonlocal fake_processors
            processor = FakeDagFileProcessorRunner._create_process(*args, **kwargs)
            fake_processors.append(processor)
            return processor

        mock_processor.side_effect = fake_processor_

        manager = DagFileProcessorManager(
            dag_directory=dag_filepath,
            dag_ids=[],
            # A reasonable large number to ensure that we trigger the deadlock
            max_runs=100,
            processor_timeout=timedelta(seconds=5),
            signal_conn=child_pipe,
            pickle_dags=False,
            async_mode=True,
        )

        try:
            thread.start()

            # If this completes without hanging, then the test is good!
            manager._run_parsing_loop()
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
    def test_send_file_processing_statsd_timing(self, statsd_timing_mock, tmp_path):
        path_to_parse = tmp_path / "temp_dag.py"
        dag_code = textwrap.dedent(
            """
        from airflow import DAG
        dag = DAG(dag_id='temp_dag', schedule='0 0 * * *')
        """
        )
        path_to_parse.write_text(dag_code)

        child_pipe, parent_pipe = multiprocessing.Pipe()

        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=path_to_parse.parent,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=async_mode,
            ),
        )

        self.run_processor_manager_one_loop(manager, parent_pipe)
        last_runtime = manager.processor.get_last_runtime(manager.processor.file_paths[0])

        child_pipe.close()
        parent_pipe.close()

        statsd_timing_mock.assert_has_calls(
            [
                mock.call("dag_processing.last_duration.temp_dag", timedelta(seconds=last_runtime)),
                mock.call(
                    "dag_processing.last_duration",
                    timedelta(seconds=last_runtime),
                    tags={"file_name": "temp_dag"},
                ),
            ],
            any_order=True,
        )

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_refresh_dags_dir_doesnt_delete_zipped_dags(self, tmp_path):
        """Test DagProcessorJobRunner._refresh_dag_dir method"""
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=TEST_DAG_FOLDER,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )
        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        zipped_dag_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
        dagbag.process_file(zipped_dag_path)
        dag = dagbag.get_dag("test_zip_dag")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        manager.processor.last_dag_dir_refresh_time = timezone.utcnow() - timedelta(minutes=10)
        manager.processor._refresh_dag_dir()
        # Assert dag not deleted in SDM
        assert SerializedDagModel.has_dag("test_zip_dag")
        # assert code not deleted
        assert DagCode.has_dag(dag.fileloc)
        # assert dag still active
        assert dag.get_is_active()

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_refresh_dags_dir_deactivates_deleted_zipped_dags(self, tmp_path):
        """Test DagProcessorJobRunner._refresh_dag_dir method"""
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=TEST_DAG_FOLDER,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )
        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        zipped_dag_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
        dagbag.process_file(zipped_dag_path)
        dag = dagbag.get_dag("test_zip_dag")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        manager.processor.last_dag_dir_refresh_time = timezone.utcnow() - timedelta(minutes=10)

        # Mock might_contain_dag to mimic deleting the python file from the zip
        with mock.patch("airflow.dag_processing.manager.might_contain_dag", return_value=False):
            manager.processor._refresh_dag_dir()

        # Assert dag removed from SDM
        assert not SerializedDagModel.has_dag("test_zip_dag")
        # assert code deleted
        assert not DagCode.has_dag(dag.fileloc)
        # assert dag deactivated
        assert not dag.get_is_active()

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    def test_refresh_dags_dir_does_not_interfer_with_dags_outside_its_subdir(self, tmp_path):
        """Test DagProcessorJobRunner._refresh_dag_dir should not update dags outside its processor_subdir"""

        dagbag = DagBag(dag_folder=tmp_path, include_examples=False)
        dag_path = os.path.join(TEST_DAGS_FOLDER, "test_miscellaneous.py")
        dagbag.process_file(dag_path)
        dag = dagbag.get_dag("miscellaneous_test_dag")
        dag.sync_to_db(processor_subdir=str(TEST_DAG_FOLDER))
        SerializedDagModel.write_dag(dag, processor_subdir=str(TEST_DAG_FOLDER))

        assert SerializedDagModel.has_dag("miscellaneous_test_dag")
        assert dag.get_is_active()
        assert DagCode.has_dag(dag.fileloc)

        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=TEST_DAG_FOLDER / "subdir2" / "subdir3",
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )
        manager.processor.last_dag_dir_refresh_time = timezone.utcnow() - timedelta(minutes=10)

        manager.processor._refresh_dag_dir()

        assert SerializedDagModel.has_dag("miscellaneous_test_dag")
        assert dag.get_is_active()
        assert DagCode.has_dag(dag.fileloc)

    @conf_vars(
        {
            ("core", "load_examples"): "False",
            ("scheduler", "standalone_dag_processor"): "True",
        }
    )
    def test_fetch_callbacks_from_database(self, tmp_path):
        """Test DagProcessorJobRunner._fetch_callbacks method"""
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        callback1 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            processor_subdir=os.fspath(tmp_path),
            run_id="123",
        )
        callback2 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            processor_subdir=os.fspath(tmp_path),
            run_id="456",
        )
        callback3 = SlaCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            processor_subdir=os.fspath(tmp_path),
        )
        callback4 = ToggleCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            processor_subdir=os.fspath(tmp_path),
        )

        with create_session() as session:
            session.add(DbCallbackRequest(callback=callback1, priority_weight=11))
            session.add(DbCallbackRequest(callback=callback2, priority_weight=10))
            session.add(DbCallbackRequest(callback=callback3, priority_weight=9))
            session.add(DbCallbackRequest(callback=callback4, priority_weight=9))

        child_pipe, parent_pipe = multiprocessing.Pipe()
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=os.fspath(tmp_path),
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=False,
            ),
        )

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)
            assert session.query(DbCallbackRequest).count() == 0

    @conf_vars(
        {
            ("core", "load_examples"): "False",
            ("scheduler", "standalone_dag_processor"): "True",
        }
    )
    def test_fetch_callbacks_for_current_dag_directory_only(self, tmp_path):
        """Test DagProcessorJobRunner._fetch_callbacks method"""
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        callback1 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            processor_subdir=os.fspath(tmp_path),
            run_id="123",
        )
        callback2 = DagCallbackRequest(
            dag_id="test_start_date_scheduling",
            full_filepath=str(dag_filepath),
            is_failure_callback=True,
            processor_subdir="/some/other/dir/",
            run_id="456",
        )

        with create_session() as session:
            session.add(DbCallbackRequest(callback=callback1, priority_weight=11))
            session.add(DbCallbackRequest(callback=callback2, priority_weight=10))

        child_pipe, parent_pipe = multiprocessing.Pipe()
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=tmp_path,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=False,
            ),
        )

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)
            assert session.query(DbCallbackRequest).count() == 1

    @conf_vars(
        {
            ("scheduler", "standalone_dag_processor"): "True",
            ("scheduler", "max_callbacks_per_loop"): "2",
            ("core", "load_examples"): "False",
        }
    )
    def test_fetch_callbacks_from_database_max_per_loop(self, tmp_path):
        """Test DagProcessorJobRunner._fetch_callbacks method"""
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        with create_session() as session:
            for i in range(5):
                callback = DagCallbackRequest(
                    dag_id="test_start_date_scheduling",
                    full_filepath=str(dag_filepath),
                    is_failure_callback=True,
                    run_id=str(i),
                    processor_subdir=os.fspath(tmp_path),
                )
                session.add(DbCallbackRequest(callback=callback, priority_weight=i))

        child_pipe, parent_pipe = multiprocessing.Pipe()
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=str(tmp_path),
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=False,
            ),
        )

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)
            assert session.query(DbCallbackRequest).count() == 3

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)
            assert session.query(DbCallbackRequest).count() == 1

    @conf_vars(
        {
            ("scheduler", "standalone_dag_processor"): "False",
            ("core", "load_examples"): "False",
        }
    )
    def test_fetch_callbacks_from_database_not_standalone(self, tmp_path):
        dag_filepath = TEST_DAG_FOLDER / "test_on_failure_callback_dag.py"

        with create_session() as session:
            callback = DagCallbackRequest(
                dag_id="test_start_date_scheduling",
                full_filepath=str(dag_filepath),
                is_failure_callback=True,
                processor_subdir=str(tmp_path),
                run_id="123",
            )
            session.add(DbCallbackRequest(callback=callback, priority_weight=10))

        child_pipe, parent_pipe = multiprocessing.Pipe()
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=tmp_path,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=False,
            ),
        )

        with create_session() as session:
            results = self.run_processor_manager_one_loop(manager, parent_pipe)

        assert (len(results)) == 0
        # Verify no callbacks removed from database.
        with create_session() as session:
            assert session.query(DbCallbackRequest).count() == 1

    def test_callback_queue(self, tmp_path):
        # given
        manager = DagProcessorJobRunner(
            job=Job(),
            processor=DagFileProcessorManager(
                dag_directory=TEST_DAG_FOLDER,
                max_runs=1,
                processor_timeout=timedelta(days=365),
                signal_conn=MagicMock(),
                dag_ids=[],
                pickle_dags=False,
                async_mode=True,
            ),
        )

        dag1_req1 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            run_id="run1",
            is_failure_callback=False,
            processor_subdir=tmp_path,
            msg=None,
        )
        dag1_req2 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            run_id="run1",
            is_failure_callback=False,
            processor_subdir=tmp_path,
            msg=None,
        )
        dag1_sla1 = SlaCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            processor_subdir=tmp_path,
        )
        dag1_sla2 = SlaCallbackRequest(
            full_filepath="/green_eggs/ham/file1.py",
            dag_id="dag1",
            processor_subdir=tmp_path,
        )

        dag2_req1 = DagCallbackRequest(
            full_filepath="/green_eggs/ham/file2.py",
            dag_id="dag2",
            run_id="run1",
            is_failure_callback=False,
            processor_subdir=tmp_path,
            msg=None,
        )

        dag3_sla1 = SlaCallbackRequest(
            full_filepath="/green_eggs/ham/file3.py",
            dag_id="dag3",
            processor_subdir=tmp_path,
        )

        # when
        manager.processor._add_callback_to_queue(dag1_req1)
        manager.processor._add_callback_to_queue(dag1_sla1)
        manager.processor._add_callback_to_queue(dag2_req1)

        # then - requests should be in manager's queue, with dag2 ahead of dag1 (because it was added last)
        assert manager.processor._file_path_queue == deque([dag2_req1.full_filepath, dag1_req1.full_filepath])
        assert set(manager.processor._callback_to_execute.keys()) == {
            dag1_req1.full_filepath,
            dag2_req1.full_filepath,
        }
        assert manager.processor._callback_to_execute[dag1_req1.full_filepath] == [dag1_req1, dag1_sla1]
        assert manager.processor._callback_to_execute[dag2_req1.full_filepath] == [dag2_req1]

        # when
        manager.processor._add_callback_to_queue(dag1_sla2)
        manager.processor._add_callback_to_queue(dag3_sla1)

        # then - since sla2 == sla1, should not have brought dag1 to the fore, and an SLA on dag3 doesn't
        # update the queue, although the callback is registered
        assert manager.processor._file_path_queue == deque([dag2_req1.full_filepath, dag1_req1.full_filepath])
        assert manager.processor._callback_to_execute[dag1_req1.full_filepath] == [dag1_req1, dag1_sla1]
        assert manager.processor._callback_to_execute[dag3_sla1.full_filepath] == [dag3_sla1]

        # when
        manager.processor._add_callback_to_queue(dag1_req2)

        # then - non-sla callback should have brought dag1 to the fore
        assert manager.processor._file_path_queue == deque([dag1_req1.full_filepath, dag2_req1.full_filepath])
        assert manager.processor._callback_to_execute[dag1_req1.full_filepath] == [
            dag1_req1,
            dag1_sla1,
            dag1_req2,
        ]


def _wait_for_processor_agent_to_complete_in_async_mode(processor_agent: DagFileProcessorAgent):
    start_timer = time.monotonic()
    while time.monotonic() - start_timer < 10:
        if processor_agent.done and all(
            [processor.done for processor in processor_agent._processors.values()]
        ):
            break
        processor_agent.heartbeat()
        time.sleep(0.1)


class TestDagFileProcessorAgent:
    def setup_method(self):
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

    def teardown_method(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        remove_list = []
        for mod in sys.modules:
            if mod not in self.old_modules:
                remove_list.append(mod)

        for mod in remove_list:
            del sys.modules[mod]

    def test_reload_module(self):
        """
        Configure the context to have logging.logging_config_class set to a fake logging
        class path, thus when reloading logging module the airflow.processor_manager
        logger should not be configured.
        """
        with settings_context(SETTINGS_FILE_VALID):
            # Launch a process through DagFileProcessorAgent, which will try
            # reload the logging module.
            test_dag_path = TEST_DAG_FOLDER / "test_scheduler_dags.py"
            async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
            log_file_loc = conf.get("logging", "DAG_PROCESSOR_MANAGER_LOG_LOCATION")

            with contextlib.suppress(OSError):
                os.remove(log_file_loc)

            # Starting dag processing with 0 max_runs to avoid redundant operations.
            processor_agent = DagFileProcessorAgent(
                test_dag_path, 0, timedelta(days=365), [], False, async_mode
            )
            processor_agent.start()
            if not async_mode:
                processor_agent.run_single_parsing_loop()

            processor_agent._process.join()
            # Since we are reloading logging config not creating this file,
            # we should expect it to be nonexistent.

            assert not os.path.isfile(log_file_loc)

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @conf_vars({("core", "load_examples"): "False"})
    def test_parse_once(self):
        clear_db_serialized_dags()
        clear_db_dags()

        test_dag_path = TEST_DAG_FOLDER / "test_scheduler_dags.py"
        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")
        processor_agent = DagFileProcessorAgent(test_dag_path, 1, timedelta(days=365), [], False, async_mode)
        processor_agent.start()
        if not async_mode:
            processor_agent.run_single_parsing_loop()
        while not processor_agent.done:
            if not async_mode:
                processor_agent.wait_until_finished()
            processor_agent.heartbeat()

        assert processor_agent.all_files_processed
        assert processor_agent.done

        with create_session() as session:
            dag_ids = session.query(DagModel.dag_id).order_by("dag_id").all()
            assert dag_ids == [("test_start_date_scheduling",), ("test_task_start_date_scheduling",)]

            dag_ids = session.query(SerializedDagModel.dag_id).order_by("dag_id").all()
            assert dag_ids == [("test_start_date_scheduling",), ("test_task_start_date_scheduling",)]

    def test_launch_process(self):
        test_dag_path = TEST_DAG_FOLDER / "test_scheduler_dags.py"
        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")

        log_file_loc = conf.get("logging", "DAG_PROCESSOR_MANAGER_LOG_LOCATION")
        with contextlib.suppress(OSError):
            os.remove(log_file_loc)

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(test_dag_path, 0, timedelta(days=365), [], False, async_mode)
        processor_agent.start()
        if not async_mode:
            processor_agent.run_single_parsing_loop()

        processor_agent._process.join()

        assert os.path.isfile(log_file_loc)

    def test_single_parsing_loop_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._process = Mock()
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.run_single_parsing_loop()

    def test_single_parsing_loop_no_process(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.run_single_parsing_loop()

    def test_single_parsing_loop_process_isnt_alive(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._process = Mock()
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process.is_alive.return_value = False
        ret_val = processor_agent.run_single_parsing_loop()
        assert not ret_val

    def test_single_parsing_loop_process_conn_error(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._process = Mock()
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process.is_alive.return_value = True
        processor_agent._parent_signal_conn.send.side_effect = ConnectionError
        ret_val = processor_agent.run_single_parsing_loop()
        assert not ret_val

    def test_get_callbacks_pipe(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        retval = processor_agent.get_callbacks_pipe()
        assert retval == processor_agent._parent_signal_conn

    def test_get_callbacks_pipe_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.get_callbacks_pipe()

    def test_wait_until_finished_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.wait_until_finished()

    def test_wait_until_finished_poll_eof_error(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.return_value = True
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.side_effect = EOFError
        ret_val = processor_agent.wait_until_finished()
        assert ret_val is None

    def test_heartbeat_no_parent_signal_conn(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent.heartbeat()

    def test_heartbeat_poll_eof_error(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.return_value = True
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.side_effect = EOFError
        ret_val = processor_agent.heartbeat()
        assert ret_val is None

    def test_heartbeat_poll_connection_error(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.return_value = True
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.side_effect = ConnectionError
        ret_val = processor_agent.heartbeat()
        assert ret_val is None

    def test_heartbeat_poll_process_message(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.poll.side_effect = [True, False]
        processor_agent._parent_signal_conn.recv = Mock()
        processor_agent._parent_signal_conn.recv.return_value = "testelem"
        with mock.patch.object(processor_agent, "_process_message"):
            processor_agent.heartbeat()
            processor_agent._process_message.assert_called_with("testelem")

    def test_process_message_invalid_type(self):
        message = "xyz"
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        with pytest.raises(RuntimeError, match="Unexpected message received of type str"):
            processor_agent._process_message(message)

    def test_heartbeat_manager(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = None
        with pytest.raises(ValueError, match="Process not started"):
            processor_agent._heartbeat_manager()

    @mock.patch("airflow.utils.process_utils.reap_process_group")
    def test_heartbeat_manager_process_restart(self, mock_pg):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process = MagicMock()
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
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
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

    def test_heartbeat_manager_terminate(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._parent_signal_conn = Mock()
        processor_agent._process = Mock()
        processor_agent._process.is_alive.return_value = True
        processor_agent.log.info = Mock()

        processor_agent.terminate()
        processor_agent._parent_signal_conn.send.assert_called_with(DagParsingSignal.TERMINATE_MANAGER)

    def test_heartbeat_manager_terminate_conn_err(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._process = Mock()
        processor_agent._process.is_alive.return_value = True
        processor_agent._parent_signal_conn = Mock()
        processor_agent._parent_signal_conn.send.side_effect = ConnectionError
        processor_agent.log.info = Mock()

        processor_agent.terminate()
        processor_agent._parent_signal_conn.send.assert_called_with(DagParsingSignal.TERMINATE_MANAGER)

    def test_heartbeat_manager_end_no_process(self):
        processor_agent = DagFileProcessorAgent("", 1, timedelta(days=365), [], False, False)
        processor_agent._process = Mock()
        processor_agent._process.__bool__ = Mock(return_value=False)
        processor_agent._process.side_effect = [None]
        processor_agent.log.warning = Mock()

        processor_agent.end()
        processor_agent.log.warning.assert_called_with("Ending without manager process.")
        processor_agent._process.join.assert_not_called()

    @conf_vars({("logging", "dag_processor_manager_log_stdout"): "True"})
    def test_log_to_stdout(self, capfd):
        test_dag_path = TEST_DAG_FOLDER / "test_scheduler_dags.py"
        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(test_dag_path, 0, timedelta(days=365), [], False, async_mode)
        processor_agent.start()
        if not async_mode:
            processor_agent.run_single_parsing_loop()

        processor_agent._process.join()
        if async_mode:
            _wait_for_processor_agent_to_complete_in_async_mode(processor_agent)

        # Capture the stdout and stderr
        out, _ = capfd.readouterr()
        assert "DAG File Processing Stats" in out

    @conf_vars({("logging", "dag_processor_manager_log_stdout"): "False"})
    def test_not_log_to_stdout(self, capfd):
        test_dag_path = TEST_DAG_FOLDER / "test_scheduler_dags.py"
        async_mode = "sqlite" not in conf.get("database", "sql_alchemy_conn")

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(test_dag_path, 0, timedelta(days=365), [], False, async_mode)
        processor_agent.start()
        if not async_mode:
            processor_agent.run_single_parsing_loop()

        processor_agent._process.join()
        if async_mode:
            _wait_for_processor_agent_to_complete_in_async_mode(processor_agent)

        # Capture the stdout and stderr
        out, _ = capfd.readouterr()
        assert "DAG File Processing Stats" not in out
