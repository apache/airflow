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
#

import os
import unittest
from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime

from airflow import AirflowException, settings
from airflow.bin.cli import CLIFactory
from airflow.utils import cli, cli_action_loggers


class TestCliUtil(unittest.TestCase):

    def test_metrics_build(self):
        func_name = 'test'
        exec_date = datetime.utcnow()
        namespace = Namespace(dag_id='foo', task_id='bar',
                              subcommand='test', execution_date=exec_date)
        metrics = cli._build_metrics(func_name, namespace)

        expected = {'user': os.environ.get('USER'),
                    'sub_command': 'test',
                    'dag_id': 'foo',
                    'task_id': 'bar',
                    'execution_date': exec_date}
        for k, v in expected.items():
            self.assertEqual(v, metrics.get(k))

        self.assertTrue(metrics.get('start_datetime') <= datetime.utcnow())
        self.assertTrue(metrics.get('full_command'))

        log_dao = metrics.get('log')
        self.assertTrue(log_dao)
        self.assertEqual(log_dao.dag_id, metrics.get('dag_id'))
        self.assertEqual(log_dao.task_id, metrics.get('task_id'))
        self.assertEqual(log_dao.execution_date, metrics.get('execution_date'))
        self.assertEqual(log_dao.owner, metrics.get('user'))

    def test_fail_function(self):
        """
        Actual function is failing and fail needs to be propagated.
        :return:
        """
        with self.assertRaises(NotImplementedError):
            fail_func(Namespace())

    def test_success_function(self):
        """
        Test success function but with failing callback.
        In this case, failure should not propagate.
        :return:
        """
        with fail_action_logger_callback():
            success_func(Namespace())

    def test_process_subdir_path_with_placeholder(self):
        self.assertEqual(os.path.join(settings.DAGS_FOLDER, 'abc'), cli.process_subdir('DAGS_FOLDER/abc'))

    def test_get_dags(self):
        parser = CLIFactory.get_parser()
        dags = cli.get_dags(parser.parse_args(['tasks', 'clear', 'example_subdag_operator', '--yes']))
        self.assertEqual(len(dags), 1)

        dags = cli.get_dags(parser.parse_args(['tasks', 'clear', 'subdag', '-dx', '--yes']))
        self.assertGreater(len(dags), 1)

        with self.assertRaises(AirflowException):
            cli.get_dags(parser.parse_args(['tasks', 'clear', 'foobar', '-dx', '--yes']))


@contextmanager
def fail_action_logger_callback():
    """
    Adding failing callback and revert it back when closed.
    :return:
    """
    tmp = cli_action_loggers.__pre_exec_callbacks[:]

    def fail_callback(**_):
        raise NotImplementedError

    cli_action_loggers.register_pre_exec_callback(fail_callback)
    yield
    cli_action_loggers.__pre_exec_callbacks = tmp


@cli.action_logging
def fail_func(_):
    raise NotImplementedError


@cli.action_logging
def success_func(_):
    pass


if __name__ == '__main__':
    unittest.main()
