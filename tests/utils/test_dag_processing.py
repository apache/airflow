# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import os
import shutil
import time

from mock import MagicMock

from airflow.utils.dag_processing import (DagFileProcessorManager,
                                          list_py_file_paths)


class TestDagFileProcessorManager(unittest.TestCase):
    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        manager = DagFileProcessorManager(dag_directory='directory', file_paths=['abc.txt'],
                                          parallelism=1, process_file_interval=1,
                                          max_runs=1, processor_factory=MagicMock().return_value)

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError(
            'DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['missing_file.txt'] = mock_processor

        manager.set_file_paths(['abc.txt'])
        self.assertDictEqual(manager._processors, {})

    def test_set_file_paths_when_processor_file_path_is_in_new_file_paths(self):
        manager = DagFileProcessorManager(dag_directory='directory', file_paths=['abc.txt'],
                                          parallelism=1, process_file_interval=1,
                                          max_runs=1, processor_factory=MagicMock().return_value)

        mock_processor = MagicMock()
        mock_processor.stop.side_effect = AttributeError(
            'DagFileProcessor object has no attribute stop')
        mock_processor.terminate.side_effect = None

        manager._processors['abc.txt'] = mock_processor

        manager.set_file_paths(['abc.txt'])
        self.assertDictEqual(manager._processors, {'abc.txt': mock_processor})

    def test_list_py_file_by_mtime(self):
        """Test list files sorted by mtime
        :return:
        """
        dir_name = './test-dags-dir'
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
        os.mkdir(dir_name)
        self.assertEqual(list_py_file_paths(dir_name), [])

        import_str = 'from airflow import DAG'

        file1 = os.path.join(dir_name, 'test-dag-2.py')
        with open(file1, 'w+') as f:
            f.write(import_str)

        time.sleep(1)

        file2 = os.path.join(dir_name, 'test-dag-1.py')
        with open(file2, 'w+') as f:
            f.write(import_str)

        self.assertEqual(list_py_file_paths(dir_name), [file1, file2])

        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)

    def test_add_files_to_queue_header(self):
        """Add file to header of queue
        :return:
        """
        manager = DagFileProcessorManager(dag_directory='directory', file_paths=['abc.txt'],
                                          parallelism=1, process_file_interval=1,
                                          max_runs=1, processor_factory=MagicMock().return_value)

        manager._file_path_queue = []

        file1, file2 = 'test-file-1', 'test-file-2'
        manager.add_files_to_queue_header([file2, file1])
        self.assertEqual(manager.file_path_queue, [file2, file1])

        # test location coverage
        manager.add_files_to_queue_header([file1])
        self.assertEqual(manager.file_path_queue, [file1, file2])

        # test add empty list
        manager.add_files_to_queue_header([])
        self.assertEqual(manager.file_path_queue, [file1, file2])
