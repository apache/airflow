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

import logging
import multiprocessing
import os
import pathlib
import random
import socket
import sys
import threading
import unittest
from datetime import datetime, timedelta
from logging.config import dictConfig
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import pytest
from freezegun import freeze_time

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
from airflow.jobs.local_task_job import LocalTaskJob as LJ
from airflow.models import DagBag, DagModel, TaskInstance as TI, errors
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.utils import timezone
from airflow.utils.callback_requests import CallbackRequest, TaskCallbackRequest
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from tests.core.test_logging_config import SETTINGS_FILE_VALID, settings_context
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

TEST_DAG_FOLDER = pathlib.Path(__file__).parent.parent / 'dags'

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class FakeDagFileProcessorRunner(DagFileProcessorProcess):
    # This fake processor will return the zombies it received in constructor
    # as its processing result w/o actually parsing anything.
    def __init__(self, file_path, pickle_dags, dag_ids, callbacks):
        super().__init__(file_path, pickle_dags, dag_ids, callbacks)
        # We need a "real" selectable handle for waitable_handle to work
        readable, writable = multiprocessing.Pipe(duplex=False)
        writable.send('abc')
        writable.close()
        self._waitable_handle = readable
        self._result = 0, 0

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
    def _create_process(file_path, callback_requests, dag_ids, pickle_dags):
        return FakeDagFileProcessorRunner(
            file_path,
            pickle_dags,
            dag_ids,
            callback_requests,
        )

    @property
    def waitable_handle(self):
        return self._waitable_handle


class TestDagFileProcessorManager:
    def setup_method(self):
        dictConfig(DEFAULT_LOGGING_CONFIG)
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

    def teardown_class(self):
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

    def run_processor_manager_one_loop(self, manager, parent_pipe):
        if not manager._async_mode:
            parent_pipe.send(DagParsingSignal.AGENT_RUN_ONCE)

        results = []

        while True:
            manager._run_parsing_loop()

            while parent_pipe.poll(timeout=0.01):
                obj = parent_pipe.recv()
                if not isinstance(obj, DagParsingStat):
                    results.append(obj)
                elif obj.done:
                    return results
            raise RuntimeError("Shouldn't get here - nothing to read, but manager not finished!")

    @conf_vars({('core', 'load_examples'): 'False'})
    def test_remove_file_clears_import_error(self, tmpdir):
        filename_to_parse = tmpdir / 'temp_dag.py'

        # Generate original import error
        with open(filename_to_parse, 'w') as file_to_parse:
            file_to_parse.writelines('an invalid airflow DAG')

        child_pipe, parent_pipe = multiprocessing.Pipe()

        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
        manager = DagFileProcessorManager(
            dag_directory=tmpdir,
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=child_pipe,
            dag_ids=[],
            pickle_dags=False,
            async_mode=async_mode,
        )

        with create_session() as session:
            self.run_processor_manager_one_loop(manager, parent_pipe)

            import_errors = session.query(errors.ImportError).all()
            assert len(import_errors) == 1

            filename_to_parse.remove()

            # Rerun the scheduler once the dag file has been removed
            self.run_processor_manager_one_loop(manager, parent_pipe)
            import_errors = session.query(errors.ImportError).all()

            assert len(import_errors) == 0
            session.rollback()

        child_pipe.close()
        parent_pipe.close()

    @conf_vars({('core', 'load_examples'): 'False'})
    def test_max_runs_when_no_files(self):

        child_pipe, parent_pipe = multiprocessing.Pipe()

        with TemporaryDirectory(prefix="empty-airflow-dags-") as dags_folder:
            async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
            manager = DagFileProcessorManager(
                dag_directory=dags_folder,
                max_runs=1,
                processor_timeout=timedelta.max,
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=async_mode,
            )

            self.run_processor_manager_one_loop(manager, parent_pipe)
        child_pipe.close()
        parent_pipe.close()

    @pytest.mark.backend("mysql", "postgres")
    def test_start_new_processes_with_same_filepath(self):
        """
        Test that when a processor already exist with a filepath, a new processor won't be created
        with that filepath. The filepath will just be removed from the list.
        """
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        file_1 = 'file_1.py'
        file_2 = 'file_2.py'
        file_3 = 'file_3.py'
        manager._file_path_queue = [file_1, file_2, file_3]

        # Mock that only one processor exists. This processor runs with 'file_1'
        manager._processors[file_1] = MagicMock()
        # Start New Processes
        manager.start_new_processes()

        # Because of the config: '[scheduler] parsing_processes = 2'
        # verify that only one extra process is created
        # and since a processor with 'file_1' already exists,
        # even though it is first in '_file_path_queue'
        # a new processor is created with 'file_2' and not 'file_1'.

        assert file_1 in manager._processors.keys()
        assert file_2 in manager._processors.keys()
        assert [file_3] == manager._file_path_queue

    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError('DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['missing_file.txt'] = mock_processor
        manager._file_stats['missing_file.txt'] = DagFileStat(0, 0, None, None, 0)

        manager.set_file_paths(['abc.txt'])
        assert manager._processors == {}

    def test_set_file_paths_when_processor_file_path_is_in_new_file_paths(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError('DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['abc.txt'] = mock_processor

        manager.set_file_paths(['abc.txt'])
        assert manager._processors == {'abc.txt': mock_processor}

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

        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == []
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == ['file_1.py', 'file_2.py', 'file_3.py', 'file_4.py']

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

        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == []
        manager.prepare_file_path_queue()

        expected_order = dag_files
        random.Random(get_hostname()).shuffle(expected_order)
        assert manager._file_path_queue == expected_order

        # Verify running it again produces same order
        manager._file_paths = []
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == expected_order

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_sorted_by_modified_time(
        self, mock_getmtime, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        """Test files are sorted by modified time"""
        paths_with_mtime = {"file_3.py": 3.0, "file_2.py": 2.0, "file_4.py": 5.0, "file_1.py": 4.0}
        dag_files = list(paths_with_mtime.keys())
        mock_getmtime.side_effect = list(paths_with_mtime.values())
        mock_find_path.return_value = dag_files

        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        manager.set_file_paths(dag_files)
        assert manager._file_path_queue == []
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == ['file_4.py', 'file_1.py', 'file_3.py', 'file_2.py']

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_file_paths_in_queue_excludes_missing_file(
        self, mock_getmtime, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        """Check that a file is not enqueued for processing if it has been deleted"""
        dag_files = ["file_3.py", "file_2.py", "file_4.py"]
        mock_getmtime.side_effect = [1.0, 2.0, FileNotFoundError()]
        mock_find_path.return_value = dag_files

        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        manager.set_file_paths(dag_files)
        manager.prepare_file_path_queue()
        assert manager._file_path_queue == ['file_2.py', 'file_3.py']

    @conf_vars({("scheduler", "file_parsing_sort_mode"): "modified_time"})
    @mock.patch("zipfile.is_zipfile", return_value=True)
    @mock.patch("airflow.utils.file.might_contain_dag", return_value=True)
    @mock.patch("airflow.utils.file.find_path_from_directory", return_value=True)
    @mock.patch("airflow.utils.file.os.path.isfile", return_value=True)
    @mock.patch("airflow.utils.file.os.path.getmtime")
    def test_recently_modified_file_is_parsed_with_mtime_mode(
        self, mock_getmtime, mock_isfile, mock_find_path, mock_might_contain_dag, mock_zipfile
    ):
        """
        Test recently updated files are processed even if min_file_process_interval is not reached
        """
        freezed_base_time = timezone.datetime(2020, 1, 5, 0, 0, 0)
        initial_file_1_mtime = (freezed_base_time - timedelta(minutes=5)).timestamp()
        dag_files = ["file_1.py"]
        mock_getmtime.side_effect = [initial_file_1_mtime]
        mock_find_path.return_value = dag_files

        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=3,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        # let's say the DAG was just parsed 2 seconds before the Freezed time
        last_finish_time = freezed_base_time - timedelta(seconds=10)
        manager._file_stats = {
            "file_1.py": DagFileStat(1, 0, last_finish_time, 1.0, 1),
        }
        with freeze_time(freezed_base_time):
            manager.set_file_paths(dag_files)
            assert manager._file_path_queue == []
            # File Path Queue will be empty as the "modified time" < "last finish time"
            manager.prepare_file_path_queue()
            assert manager._file_path_queue == []

        # Simulate the DAG modification by using modified_time which is greater
        # than the last_parse_time but still less than now - min_file_process_interval
        file_1_new_mtime = freezed_base_time - timedelta(seconds=5)
        file_1_new_mtime_ts = file_1_new_mtime.timestamp()
        with freeze_time(freezed_base_time):
            manager.set_file_paths(dag_files)
            assert manager._file_path_queue == []
            # File Path Queue will be empty as the "modified time" < "last finish time"
            mock_getmtime.side_effect = [file_1_new_mtime_ts]
            manager.prepare_file_path_queue()
            # Check that file is added to the queue even though file was just recently passed
            assert manager._file_path_queue == ["file_1.py"]
            assert last_finish_time < file_1_new_mtime
            assert (
                manager._file_process_interval
                > (freezed_base_time - manager.get_last_finish_time("file_1.py")).total_seconds()
            )

    def test_find_zombies(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        dagbag = DagBag(TEST_DAG_FOLDER, read_dags_from_db=False)
        with create_session() as session:
            session.query(LJ).delete()
            dag = dagbag.get_dag('example_branch_operator')
            dag.sync_to_db()
            task = dag.get_task(task_id='run_this_first')

            dag_run = dag.create_dagrun(
                state=DagRunState.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )

            ti = TI(task, run_id=dag_run.run_id, state=State.RUNNING)
            local_job = LJ(ti)
            local_job.state = State.SHUTDOWN

            session.add(local_job)
            session.flush()

            ti.job_id = local_job.id
            session.add(ti)
            session.flush()

            manager._last_zombie_query_time = timezone.utcnow() - timedelta(
                seconds=manager._zombie_threshold_secs + 1
            )
            manager._find_zombies()
            requests = manager._callback_to_execute[dag.fileloc]
            assert 1 == len(requests)
            assert requests[0].full_filepath == dag.fileloc
            assert requests[0].msg == f"Detected {ti} as zombie"
            assert requests[0].is_failure_callback is True
            assert isinstance(requests[0].simple_task_instance, SimpleTaskInstance)
            assert ti.dag_id == requests[0].simple_task_instance.dag_id
            assert ti.task_id == requests[0].simple_task_instance.task_id
            assert ti.run_id == requests[0].simple_task_instance.run_id

            session.query(TI).delete()
            session.query(LJ).delete()

    @mock.patch('airflow.dag_processing.manager.DagFileProcessorProcess')
    def test_handle_failure_callback_with_zombies_are_correctly_passed_to_dag_file_processor(
        self, mock_processor
    ):
        """
        Check that the same set of failure callback with zombies are passed to the dag
        file processors until the next zombie detection logic is invoked.
        """
        test_dag_path = TEST_DAG_FOLDER / 'test_example_bash_operator.py'
        with conf_vars({('scheduler', 'parsing_processes'): '1', ('core', 'load_examples'): 'False'}):
            dagbag = DagBag(test_dag_path, read_dags_from_db=False)
            with create_session() as session:
                session.query(LJ).delete()
                dag = dagbag.get_dag('test_example_bash_operator')
                dag.sync_to_db()

                dag_run = dag.create_dagrun(
                    state=DagRunState.RUNNING,
                    execution_date=DEFAULT_DATE,
                    run_type=DagRunType.SCHEDULED,
                    session=session,
                )
                task = dag.get_task(task_id='run_this_last')

                ti = TI(task, run_id=dag_run.run_id, state=State.RUNNING)
                local_job = LJ(ti)
                local_job.state = State.SHUTDOWN
                session.add(local_job)
                session.flush()

                # TODO: If there was an actual Relationship between TI and Job
                # we wouldn't need this extra commit
                session.add(ti)
                ti.job_id = local_job.id
                session.flush()

                expected_failure_callback_requests = [
                    TaskCallbackRequest(
                        full_filepath=dag.fileloc,
                        simple_task_instance=SimpleTaskInstance(ti),
                        msg="Message",
                    )
                ]

            test_dag_path = TEST_DAG_FOLDER / 'test_example_bash_operator.py'

            child_pipe, parent_pipe = multiprocessing.Pipe()
            async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')

            fake_processors = []

            def fake_processor_(*args, **kwargs):
                nonlocal fake_processors
                processor = FakeDagFileProcessorRunner._create_process(*args, **kwargs)
                fake_processors.append(processor)
                return processor

            mock_processor.side_effect = fake_processor_

            manager = DagFileProcessorManager(
                dag_directory=test_dag_path,
                max_runs=1,
                processor_timeout=timedelta.max,
                signal_conn=child_pipe,
                dag_ids=[],
                pickle_dags=False,
                async_mode=async_mode,
            )

            self.run_processor_manager_one_loop(manager, parent_pipe)

            if async_mode:
                # Once for initial parse, and then again for the add_callback_to_queue
                assert len(fake_processors) == 2
                assert fake_processors[0]._file_path == str(test_dag_path)
                assert fake_processors[0]._callback_requests == []
            else:
                assert len(fake_processors) == 1

            assert fake_processors[-1]._file_path == str(test_dag_path)
            callback_requests = fake_processors[-1]._callback_requests
            assert {zombie.simple_task_instance.key for zombie in expected_failure_callback_requests} == {
                result.simple_task_instance.key for result in callback_requests
            }

            child_pipe.close()
            parent_pipe.close()

    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.pid", new_callable=PropertyMock)
    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.kill")
    def test_kill_timed_out_processors_kill(self, mock_kill, mock_pid):
        mock_pid.return_value = 1234
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta(seconds=5),
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        processor = DagFileProcessorProcess('abc.txt', False, [], [])
        processor._start_time = timezone.make_aware(datetime.min)
        manager._processors = {'abc.txt': processor}
        manager._kill_timed_out_processors()
        mock_kill.assert_called_once_with()

    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess.pid", new_callable=PropertyMock)
    @mock.patch("airflow.dag_processing.processor.DagFileProcessorProcess")
    def test_kill_timed_out_processors_no_kill(self, mock_dag_file_processor, mock_pid):
        mock_pid.return_value = 1234
        manager = DagFileProcessorManager(
            dag_directory='directory',
            max_runs=1,
            processor_timeout=timedelta(seconds=5),
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )

        processor = DagFileProcessorProcess('abc.txt', False, [], [])
        processor._start_time = timezone.make_aware(datetime.max)
        manager._processors = {'abc.txt': processor}
        manager._kill_timed_out_processors()
        mock_dag_file_processor.kill.assert_not_called()

    @conf_vars({('core', 'load_examples'): 'False'})
    @pytest.mark.execution_timeout(10)
    def test_dag_with_system_exit(self):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = 'exit_test_dag'
        dag_directory = TEST_DAG_FOLDER.parent / 'dags_with_system_exit'

        # Delete the one valid DAG/SerializedDAG, and check that it gets re-created
        clear_db_dags()
        clear_db_serialized_dags()

        child_pipe, parent_pipe = multiprocessing.Pipe()

        manager = DagFileProcessorManager(
            dag_directory=dag_directory,
            dag_ids=[],
            max_runs=1,
            processor_timeout=timedelta(seconds=5),
            signal_conn=child_pipe,
            pickle_dags=False,
            async_mode=True,
        )

        manager._run_parsing_loop()

        result = None
        while parent_pipe.poll(timeout=None):
            result = parent_pipe.recv()
            if isinstance(result, DagParsingStat) and result.done:
                break

        # Three files in folder should be processed
        assert sum(stat.run_count for stat in manager._file_stats.values()) == 3

        with create_session() as session:
            assert session.query(DagModel).get(dag_id) is not None

    @conf_vars({('core', 'load_examples'): 'False'})
    @pytest.mark.backend("mysql", "postgres")
    @pytest.mark.execution_timeout(30)
    @mock.patch('airflow.dag_processing.manager.DagFileProcessorProcess')
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
            n = 0
            while True:
                if exit_event.is_set():
                    break

                req = CallbackRequest(str(dag_filepath))
                try:
                    logging.debug("Sending CallbackRequests %d", n + 1)
                    pipe.send(req)
                except TypeError:
                    # This is actually the error you get when the parent pipe
                    # is closed! Nicely handled, eh?
                    break
                except OSError:
                    break
                n += 1
                logging.debug("   Sent %d CallbackRequests", n)

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
            logging.info("Closing pipes")
            parent_pipe.close()
            child_pipe.close()
            thread.join(timeout=1.0)

    @conf_vars({('core', 'load_examples'): 'False'})
    @mock.patch('airflow.dag_processing.manager.Stats.timing')
    def test_send_file_processing_statsd_timing(self, statsd_timing_mock, tmpdir):
        filename_to_parse = tmpdir / 'temp_dag.py'
        dag_code = dedent(
            """
        from airflow import DAG
        dag = DAG(dag_id='temp_dag', schedule_interval='0 0 * * *')
        """
        )
        with open(filename_to_parse, 'w') as file_to_parse:
            file_to_parse.writelines(dag_code)

        child_pipe, parent_pipe = multiprocessing.Pipe()

        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
        manager = DagFileProcessorManager(
            dag_directory=tmpdir,
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=child_pipe,
            dag_ids=[],
            pickle_dags=False,
            async_mode=async_mode,
        )

        self.run_processor_manager_one_loop(manager, parent_pipe)
        last_runtime = manager.get_last_runtime(manager.file_paths[0])

        child_pipe.close()
        parent_pipe.close()

        statsd_timing_mock.assert_called_with('dag_processing.last_duration.temp_dag', last_runtime)

    def test_refresh_dags_dir_doesnt_delete_zipped_dags(self, tmpdir):
        """Test DagFileProcessorManager._refresh_dag_dir method"""
        manager = DagFileProcessorManager(
            dag_directory=TEST_DAG_FOLDER,
            max_runs=1,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            dag_ids=[],
            pickle_dags=False,
            async_mode=True,
        )
        dagbag = DagBag(dag_folder=tmpdir, include_examples=False)
        zipped_dag_path = os.path.join(TEST_DAGS_FOLDER, "test_zip.zip")
        dagbag.process_file(zipped_dag_path)
        dag = dagbag.get_dag("test_zip_dag")
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag)
        manager.last_dag_dir_refresh_time = timezone.utcnow() - timedelta(minutes=10)
        manager._refresh_dag_dir()
        # Assert dag not deleted in SDM
        assert SerializedDagModel.has_dag('test_zip_dag')
        # assert code not delted
        assert DagCode.has_dag(dag.fileloc)


class TestDagFileProcessorAgent(unittest.TestCase):
    def setUp(self):
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        remove_list = []
        for mod in sys.modules:
            if mod not in self.old_modules:
                remove_list.append(mod)

        for mod in remove_list:
            del sys.modules[mod]

    @staticmethod
    def _processor_factory(file_path, zombies, dag_ids, pickle_dags):
        return DagFileProcessorProcess(file_path, pickle_dags, dag_ids, zombies)

    def test_reload_module(self):
        """
        Configure the context to have logging.logging_config_class set to a fake logging
        class path, thus when reloading logging module the airflow.processor_manager
        logger should not be configured.
        """
        with settings_context(SETTINGS_FILE_VALID):
            # Launch a process through DagFileProcessorAgent, which will try
            # reload the logging module.
            test_dag_path = TEST_DAG_FOLDER / 'test_scheduler_dags.py'
            async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
            log_file_loc = conf.get('logging', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')

            try:
                os.remove(log_file_loc)
            except OSError:
                pass

            # Starting dag processing with 0 max_runs to avoid redundant operations.
            processor_agent = DagFileProcessorAgent(test_dag_path, 0, timedelta.max, [], False, async_mode)
            processor_agent.start()
            if not async_mode:
                processor_agent.run_single_parsing_loop()

            processor_agent._process.join()
            # Since we are reloading logging config not creating this file,
            # we should expect it to be nonexistent.

            assert not os.path.isfile(log_file_loc)

    @conf_vars({('core', 'load_examples'): 'False'})
    def test_parse_once(self):
        clear_db_serialized_dags()
        clear_db_dags()

        test_dag_path = TEST_DAG_FOLDER / 'test_scheduler_dags.py'
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
        processor_agent = DagFileProcessorAgent(test_dag_path, 1, timedelta.max, [], False, async_mode)
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
            assert dag_ids == [('test_start_date_scheduling',), ('test_task_start_date_scheduling',)]

            dag_ids = session.query(SerializedDagModel.dag_id).order_by("dag_id").all()
            assert dag_ids == [('test_start_date_scheduling',), ('test_task_start_date_scheduling',)]

    def test_launch_process(self):
        test_dag_path = TEST_DAG_FOLDER / 'test_scheduler_dags.py'
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')

        log_file_loc = conf.get('logging', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')
        try:
            os.remove(log_file_loc)
        except OSError:
            pass

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(test_dag_path, 0, timedelta.max, [], False, async_mode)
        processor_agent.start()
        if not async_mode:
            processor_agent.run_single_parsing_loop()

        processor_agent._process.join()

        assert os.path.isfile(log_file_loc)
