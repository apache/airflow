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

import os
import unittest

from mock import MagicMock

from airflow import configuration as conf
from airflow.jobs import DagFileProcessor
from airflow.utils.dag_processing import DagFileProcessorAgent
from airflow.utils.dag_processing import DagFileProcessorManager

TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.pardir, 'dags')


class TestDagFileProcessorManager(unittest.TestCase):
    def test_set_file_paths_when_processor_file_path_not_in_new_file_paths(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
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

    def test_parse_once(self):
        def processor_factory(file_path, zombies):
            return DagFileProcessor(file_path,
                                    False,
                                    [],
                                    zombies)

        test_dag_path = os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py')
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')
        processor_agent = DagFileProcessorAgent(test_dag_path,
                                                [test_dag_path],
                                                1,
                                                processor_factory,
                                                async_mode)
        processor_agent.start()
        parsing_result = []
        while not processor_agent.done:
            print(processor_agent.done)
            if not async_mode:
                processor_agent.heartbeat()
                processor_agent.wait_until_finished()
            parsing_result.extend(processor_agent.harvest_simple_dags())

        dag_ids = [result.dag_id for result in parsing_result]
        self.assertEqual(dag_ids.count('test_start_date_scheduling'), 1)
