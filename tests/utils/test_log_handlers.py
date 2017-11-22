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

import copy
import logging
import logging.config
import mock
import os
import unittest

from airflow.models import TaskInstance, DAG
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.timezone import datetime
from airflow.utils.log.file_task_handler import FileTaskHandler

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = 'airflow.task'
FILE_TASK_HANDLER = 'file.task'


class TestFileTaskLogHandler(unittest.TestCase):

    def setUp(self):
        super(TestFileTaskLogHandler, self).setUp()
        # We use file task handler by default.
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)

    def test_default_task_logging_setup(self):
        # file task handler is used by default.
        logger = logging.getLogger(TASK_LOGGER)
        handlers = logger.handlers
        self.assertEqual(len(handlers), 1)
        handler = handlers[0]
        self.assertEqual(handler.name, FILE_TASK_HANDLER)

    def test_file_task_handler(self):
        dag = DAG('dag_for_testing_file_task_handler', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='task_for_testing_file_log_handler', dag=dag)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        logger = logging.getLogger(TASK_LOGGER)
        file_handler = next((handler for handler in logger.handlers
                             if handler.name == FILE_TASK_HANDLER), None)
        self.assertIsNotNone(file_handler)

        file_handler.set_context(ti)
        self.assertIsNotNone(file_handler.handler)
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        self.assertTrue(os.path.isfile(log_filename))

        logger.info("test")
        ti.run()

        self.assertTrue(hasattr(file_handler, 'read'))
        # Return value of read must be a list.
        logs = file_handler.read(ti)
        self.assertTrue(isinstance(logs, list))
        self.assertEqual(len(logs), 1)

        # Remove the generated tmp log file.
        os.remove(log_filename)


class TestFilenameRendering(unittest.TestCase):

    def setUp(self):
        dag = DAG('dag_for_testing_filename_rendering', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='task_for_testing_filename_rendering', dag=dag)
        self.ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    def test_python_formatting(self):
        expected_filename = 'dag_for_testing_filename_rendering/task_for_testing_filename_rendering/%s/42.log' % DEFAULT_DATE.isoformat()

        fth = FileTaskHandler('', '{dag_id}/{task_id}/{execution_date}/{try_number}.log')
        rendered_filename = fth._render_filename(self.ti, 42)
        self.assertEqual(expected_filename, rendered_filename)

    def test_jinja_rendering(self):
        expected_filename = 'dag_for_testing_filename_rendering/task_for_testing_filename_rendering/%s/42.log' % DEFAULT_DATE.isoformat()

        fth = FileTaskHandler('', '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log')
        rendered_filename = fth._render_filename(self.ti, 42)
        self.assertEqual(expected_filename, rendered_filename)
