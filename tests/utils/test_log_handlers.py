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

import logging
import logging.config
import os
import unittest

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import create_session
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.state import State
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = 'airflow.task'
FILE_TASK_HANDLER = 'task'


class TestFileTaskLogHandler(unittest.TestCase):

    def cleanUp(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def setUp(self):
        super().setUp()
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False
        self.cleanUp()
        # We use file task handler by default.

    def tearDown(self):
        self.cleanUp()
        super().tearDown()

    def test_default_task_logging_setup(self):
        # file task handler is used by default.
        logger = logging.getLogger(TASK_LOGGER)
        handlers = logger.handlers
        self.assertEqual(len(handlers), 1)
        handler = handlers[0]
        self.assertEqual(handler.name, FILE_TASK_HANDLER)

    def test_file_task_handler(self):
        def task_callable(ti, **kwargs):
            ti.log.info("test")
        dag = DAG('dag_for_testing_file_task_handler', start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id='task_for_testing_file_log_handler',
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        logger = ti.log
        ti.log.disabled = False

        file_handler = next((handler for handler in logger.handlers
                             if handler.name == FILE_TASK_HANDLER), None)
        self.assertIsNotNone(file_handler)

        set_context(logger, ti)
        self.assertIsNotNone(file_handler.handler)
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        self.assertTrue(os.path.isfile(log_filename))
        self.assertTrue(log_filename.endswith("1.log"), log_filename)

        ti.run(ignore_ti_state=True)

        file_handler.flush()
        file_handler.close()

        self.assertTrue(hasattr(file_handler, 'read'))
        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        self.assertTrue(isinstance(logs, list))
        self.assertTrue(isinstance(metadatas, list))
        self.assertEqual(len(logs), 1)
        self.assertEqual(len(logs), len(metadatas))
        self.assertTrue(isinstance(metadatas[0], dict))
        target_re = r'\n\[[^\]]+\] {test_log_handlers.py:\d+} INFO - test\n'

        # We should expect our log line from the callable above to appear in
        # the logs we read back
        self.assertRegex(
            logs[0],
            target_re,
            "Logs were " + str(logs)
        )

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler_running(self):
        def task_callable(ti, **kwargs):
            ti.log.info("test")
        dag = DAG('dag_for_testing_file_task_handler', start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id='task_for_testing_file_log_handler',
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.try_number = 2
        ti.state = State.RUNNING

        logger = ti.log
        ti.log.disabled = False

        file_handler = next((handler for handler in logger.handlers
                             if handler.name == FILE_TASK_HANDLER), None)
        self.assertIsNotNone(file_handler)

        set_context(logger, ti)
        self.assertIsNotNone(file_handler.handler)
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        self.assertTrue(os.path.isfile(log_filename))
        self.assertTrue(log_filename.endswith("2.log"), log_filename)

        logger.info("Test")

        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        self.assertTrue(isinstance(logs, list))
        # Logs for running tasks should show up too.
        self.assertTrue(isinstance(logs, list))
        self.assertTrue(isinstance(metadatas, list))
        self.assertEqual(len(logs), 2)
        self.assertEqual(len(logs), len(metadatas))
        self.assertTrue(isinstance(metadatas[0], dict))

        # Remove the generated tmp log file.
        os.remove(log_filename)


class TestFilenameRendering(unittest.TestCase):

    def setUp(self):
        dag = DAG('dag_for_testing_filename_rendering', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='task_for_testing_filename_rendering', dag=dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    def test_python_formatting(self):
        expected_filename = \
            'dag_for_testing_filename_rendering/task_for_testing_filename_rendering/%s/42.log' \
            % DEFAULT_DATE.isoformat()

        fth = FileTaskHandler('', '{dag_id}/{task_id}/{execution_date}/{try_number}.log')
        rendered_filename = fth._render_filename(self.ti, 42)
        self.assertEqual(expected_filename, rendered_filename)

    def test_jinja_rendering(self):
        expected_filename = \
            'dag_for_testing_filename_rendering/task_for_testing_filename_rendering/%s/42.log' \
            % DEFAULT_DATE.isoformat()

        fth = FileTaskHandler('', '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log')
        rendered_filename = fth._render_filename(self.ti, 42)
        self.assertEqual(expected_filename, rendered_filename)
