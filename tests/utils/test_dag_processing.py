# -*- coding: utf-8 -*-
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

from datetime import (datetime, timedelta)
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock
from unittest.mock import (MagicMock, PropertyMock)

from airflow import configuration as conf
from airflow.jobs import DagFileProcessor
from airflow.utils import timezone
from airflow.utils.dag_processing import (DagFileProcessorAgent, DagFileProcessorManager,
                                          correct_maybe_zipped)

TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.pardir, 'dags')

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

SETTINGS_FILE_VALID = """
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'task': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers': {
        'airflow': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },
    }
}
"""

SETTINGS_DEFAULT_NAME = 'custom_airflow_local_settings'


class settings_context:
    """
    Sets a settings file and puts it in the Python classpath

    :param content:
          The content of the settings file
    """

    def __init__(self, content, dir=None, name='LOGGING_CONFIG'):
        self.content = content
        self.settings_root = tempfile.mkdtemp()
        filename = "{}.py".format(SETTINGS_DEFAULT_NAME)

        if dir:
            # Replace slashes by dots
            self.module = dir.replace('/', '.') + '.' + SETTINGS_DEFAULT_NAME + '.' + name

            # Create the directory structure
            dir_path = os.path.join(self.settings_root, dir)
            pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

            # Add the __init__ for the directories
            # This is required for Python 2.7
            basedir = self.settings_root
            for part in dir.split('/'):
                open(os.path.join(basedir, '__init__.py'), 'w').close()
                basedir = os.path.join(basedir, part)
            open(os.path.join(basedir, '__init__.py'), 'w').close()

            self.settings_file = os.path.join(dir_path, filename)
        else:
            self.module = SETTINGS_DEFAULT_NAME + '.' + name
            self.settings_file = os.path.join(self.settings_root, filename)

    def __enter__(self):
        with open(self.settings_file, 'w') as handle:
            handle.writelines(self.content)
        sys.path.append(self.settings_root)
        conf.set(
            'core',
            'logging_config_class',
            self.module
        )
        return self.settings_file

    def __exit__(self, *exc_info):
        # shutil.rmtree(self.settings_root)
        # Reset config
        conf.set('core', 'logging_config_class', '')
        sys.path.remove(self.settings_root)


class TestDagFileProcessorManager(unittest.TestCase):
    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            stat_queue=MagicMock(),
            result_queue=MagicMock,
            async_mode=True)

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError(
            'DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['missing_file.txt'] = mock_processor

        manager.set_file_paths(['abc.txt'])
        self.assertDictEqual(manager._processors, {})

    def test_set_file_paths_when_processor_file_path_is_in_new_file_paths(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
            processor_timeout=timedelta.max,
            signal_conn=MagicMock(),
            stat_queue=MagicMock(),
            result_queue=MagicMock,
            async_mode=True)

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError(
            'DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['abc.txt'] = mock_processor

        manager.set_file_paths(['abc.txt'])
        self.assertDictEqual(manager._processors, {'abc.txt': mock_processor})

    @mock.patch("airflow.jobs.DagFileProcessor.pid", new_callable=PropertyMock)
    @mock.patch("airflow.jobs.DagFileProcessor.kill")
    def test_kill_timed_out_processors_kill(self, mock_kill, mock_pid):
        mock_pid.return_value = 1234
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
            processor_timeout=timedelta(seconds=5),
            signal_conn=MagicMock(),
            stat_queue=MagicMock(),
            result_queue=MagicMock,
            async_mode=True)

        processor = DagFileProcessor('abc.txt', False, [])
        processor._start_time = timezone.make_aware(datetime.min)
        manager._processors = {'abc.txt': processor}
        manager._kill_timed_out_processors()
        mock_kill.assert_called_once()

    @mock.patch("airflow.jobs.DagFileProcessor.pid", new_callable=PropertyMock)
    @mock.patch("airflow.jobs.scheduler_job.DagFileProcessor")
    def test_kill_timed_out_processors_no_kill(self, mock_dag_file_processor, mock_pid):
        mock_pid.return_value = 1234
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
            processor_timeout=timedelta(seconds=5),
            signal_conn=MagicMock(),
            stat_queue=MagicMock(),
            result_queue=MagicMock,
            async_mode=True)

        processor = DagFileProcessor('abc.txt', False, [])
        processor._start_time = timezone.make_aware(datetime.max)
        manager._processors = {'abc.txt': processor}
        manager._kill_timed_out_processors()
        mock_dag_file_processor.kill.assert_not_called()


class TestDagFileProcessorAgent(unittest.TestCase):
    def setUp(self):
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for m in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[m]

    def test_reload_module(self):
        """
        Configure the context to have core.logging_config_class set to a fake logging
        class path, thus when reloading logging module the airflow.processor_manager
        logger should not be configured.
        """
        with settings_context(SETTINGS_FILE_VALID):
            # Launch a process through DagFileProcessorAgent, which will try
            # reload the logging module.
            def processor_factory(file_path):
                return DagFileProcessor(file_path,
                                        False,
                                        [])

            test_dag_path = os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py')
            async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')

            log_file_loc = conf.get('core', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')
            try:
                os.remove(log_file_loc)
            except OSError:
                pass

            # Starting dag processing with 0 max_runs to avoid redundant operations.
            processor_agent = DagFileProcessorAgent(test_dag_path,
                                                    [],
                                                    0,
                                                    processor_factory,
                                                    timedelta.max,
                                                    async_mode)
            manager_process = \
                processor_agent._launch_process(processor_agent._dag_directory,
                                                processor_agent._file_paths,
                                                processor_agent._max_runs,
                                                processor_agent._processor_factory,
                                                processor_agent._processor_timeout,
                                                processor_agent._child_signal_conn,
                                                processor_agent._stat_queue,
                                                processor_agent._result_queue,
                                                processor_agent._async_mode)
            if not async_mode:
                processor_agent.heartbeat()

            manager_process.join()

            # Since we are reloading logging config not creating this file,
            # we should expect it to be nonexistent.
            self.assertFalse(os.path.isfile(log_file_loc))

    def test_parse_once(self):
        def processor_factory(file_path):
            return DagFileProcessor(file_path,
                                    False,
                                    [])

        test_dag_path = os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py')
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
        processor_agent = DagFileProcessorAgent(test_dag_path,
                                                [test_dag_path],
                                                1,
                                                processor_factory,
                                                timedelta.max,
                                                async_mode)
        processor_agent.start()
        parsing_result = []
        while not processor_agent.done:
            if not async_mode:
                processor_agent.heartbeat()
                processor_agent.wait_until_finished()
            parsing_result.extend(processor_agent.harvest_simple_dags())

        dag_ids = [result.dag_id for result in parsing_result]
        self.assertEqual(dag_ids.count('test_start_date_scheduling'), 1)

    def test_launch_process(self):
        def processor_factory(file_path):
            return DagFileProcessor(file_path,
                                    False,
                                    [])

        test_dag_path = os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py')
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')

        log_file_loc = conf.get('core', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')
        try:
            os.remove(log_file_loc)
        except OSError:
            pass

        # Starting dag processing with 0 max_runs to avoid redundant operations.
        processor_agent = DagFileProcessorAgent(test_dag_path,
                                                [],
                                                0,
                                                processor_factory,
                                                timedelta.max,
                                                async_mode)
        manager_process = \
            processor_agent._launch_process(processor_agent._dag_directory,
                                            processor_agent._file_paths,
                                            processor_agent._max_runs,
                                            processor_agent._processor_factory,
                                            processor_agent._processor_timeout,
                                            processor_agent._child_signal_conn,
                                            processor_agent._stat_queue,
                                            processor_agent._result_queue,
                                            processor_agent._async_mode)
        if not async_mode:
            processor_agent.heartbeat()

        manager_process.join()

        self.assertTrue(os.path.isfile(log_file_loc))


class TestCorrectMaybeZipped(unittest.TestCase):
    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file(self, mocked_is_zipfile):
        path = '/path/to/some/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        self.assertEqual(dag_folder, path)

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_normal_file_with_zip_in_name(self, mocked_is_zipfile):
        path = '/path/to/fakearchive.zip.other/file.txt'
        mocked_is_zipfile.return_value = False

        dag_folder = correct_maybe_zipped(path)

        self.assertEqual(dag_folder, path)

    @mock.patch("zipfile.is_zipfile")
    def test_correct_maybe_zipped_archive(self, mocked_is_zipfile):
        path = '/path/to/archive.zip/deep/path/to/file.txt'
        mocked_is_zipfile.return_value = True

        dag_folder = correct_maybe_zipped(path)

        assert mocked_is_zipfile.call_count == 1
        (args, kwargs) = mocked_is_zipfile.call_args_list[0]
        self.assertEqual('/path/to/archive.zip', args[0])

        self.assertEqual(dag_folder, '/path/to/archive.zip')
