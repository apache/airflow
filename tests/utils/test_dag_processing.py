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
from datetime import timedelta

from mock import MagicMock

from airflow import configuration as conf
from airflow.jobs import DagFileProcessor
from airflow.jobs import LocalTaskJob as LJ
from airflow.models import DagBag, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.dag_processing import (DagFileProcessorAgent, DagFileProcessorManager,
                                          SimpleTaskInstance)
from airflow.utils.db import create_session
from airflow.utils.state import State

TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.pardir, 'dags')

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


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

    def test_find_zombies(self):
        manager = DagFileProcessorManager(
            dag_directory='directory',
            file_paths=['abc.txt'],
            max_runs=1,
            processor_factory=MagicMock().return_value,
            signal_conn=MagicMock(),
            stat_queue=MagicMock(),
            result_queue=MagicMock,
            async_mode=True)

        dagbag = DagBag(TEST_DAG_FOLDER)
        with create_session() as session:
            session.query(LJ).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TI(task, DEFAULT_DATE, State.RUNNING)
            lj = LJ(ti)
            lj.state = State.SHUTDOWN
            lj.id = 1
            ti.job_id = lj.id

            session.add(lj)
            session.add(ti)
            session.commit()

            manager._last_zombie_query_time = timezone.utcnow() - timedelta(
                seconds=manager._zombie_threshold_secs + 1)
            zombies = manager._find_zombies()
            self.assertEquals(1, len(zombies))
            self.assertIsInstance(zombies[0], SimpleTaskInstance)
            self.assertEquals(ti.dag_id, zombies[0].dag_id)
            self.assertEquals(ti.task_id, zombies[0].task_id)
            self.assertEquals(ti.execution_date, zombies[0].execution_date)

            session.query(TI).delete()
            session.query(LJ).delete()


class TestDagFileProcessorAgent(unittest.TestCase):
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
            if not async_mode:
                processor_agent.heartbeat()
                processor_agent.wait_until_finished()
            parsing_result.extend(processor_agent.harvest_simple_dags())

        dag_ids = [result.dag_id for result in parsing_result]
        self.assertEqual(dag_ids.count('test_start_date_scheduling'), 1)

    def test_launch_process(self):
        def processor_factory(file_path, zombies):
            return DagFileProcessor(file_path,
                                    False,
                                    [],
                                    zombies)

        test_dag_path = os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py')
        async_mode = 'sqlite' not in conf.get('core', 'sql_alchemy_conn')

        log_file_loc = conf.get('core', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')
        try:
            os.remove(log_file_loc)
        except OSError:
            pass

        # Starting dag processing with 0 max_runs to avoid redundent operations.
        processor_agent = DagFileProcessorAgent(test_dag_path,
                                                [],
                                                0,
                                                processor_factory,
                                                async_mode)
        manager_process = \
            processor_agent._launch_process(processor_agent._dag_directory,
                                            processor_agent._file_paths,
                                            processor_agent._max_runs,
                                            processor_agent._processor_factory,
                                            processor_agent._child_signal_conn,
                                            processor_agent._stat_queue,
                                            processor_agent._result_queue,
                                            processor_agent._async_mode)
        if not async_mode:
            processor_agent.heartbeat()

        manager_process.join()

        self.assertTrue(os.path.isfile(log_file_loc))
