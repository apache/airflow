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
import unittest

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DAG, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

DEFAULT_DATE = datetime(2019, 1, 1)
TASK_LOGGER = 'airflow.task'
TASK_HANDLER = 'task'
TASK_HANDLER_CLASS = 'airflow.utils.log.task_handler_with_custom_formatter.TaskHandlerWithCustomFormatter'
PREV_TASK_HANDLER = DEFAULT_LOGGING_CONFIG['handlers']['task']


class TestTaskHandlerWithCustomFormatter(unittest.TestCase):
    def setUp(self):
        super().setUp()
        DEFAULT_LOGGING_CONFIG['handlers']['task'] = {
            'class': TASK_HANDLER_CLASS,
            'formatter': 'airflow',
            'stream': 'sys.stdout',
        }

        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False

    def tearDown(self):
        super().tearDown()
        DEFAULT_LOGGING_CONFIG['handlers']['task'] = PREV_TASK_HANDLER

    @conf_vars({('logging', 'task_log_prefix_template'): "{{ti.dag_id}}-{{ti.task_id}}"})
    def test_formatter(self):
        dag = DAG('test_dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='test_task', dag=dag)
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        logger = ti.log
        ti.log.disabled = False
        handler = next((handler for handler in logger.handlers if handler.name == TASK_HANDLER), None)
        self.assertIsNotNone(handler)

        # setting the expected value of the formatter
        expected_formatter_value = "test_dag-test_task:" + handler.formatter._fmt
        set_context(logger, ti)
        self.assertEqual(expected_formatter_value, handler.formatter._fmt)
