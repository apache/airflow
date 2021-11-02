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
import json
import os
import sys
import unittest
from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime
from unittest import mock

from parameterized import parameterized

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.utils import cli, cli_action_loggers


class TestCliUtil(unittest.TestCase):
    def test_metrics_build(self):
        func_name = 'test'
        exec_date = datetime.utcnow()
        namespace = Namespace(dag_id='foo', task_id='bar', subcommand='test', execution_date=exec_date)
        metrics = cli._build_metrics(func_name, namespace)

        expected = {
            'user': os.environ.get('USER'),
            'sub_command': 'test',
            'dag_id': 'foo',
            'task_id': 'bar',
            'execution_date': exec_date,
        }
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
        dags = cli.get_dags(None, "example_subdag_operator")
        self.assertEqual(len(dags), 1)

        dags = cli.get_dags(None, "subdag", True)
        self.assertGreater(len(dags), 1)

        with self.assertRaises(AirflowException):
            cli.get_dags(None, "foobar", True)

    @parameterized.expand(
        [
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password ********",
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p ********",
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin --password=********",
            ),
            (
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=test",
                "airflow users create -u test2 -l doe -f jon -e jdoe@apache.org -r admin -p=********",
            ),
            (
                "airflow connections add dsfs --conn-login asd --conn-password test --conn-type google",
                "airflow connections add dsfs --conn-login asd --conn-password ******** --conn-type google",
            ),
        ]
    )
    def test_cli_create_user_supplied_password_is_masked(self, given_command, expected_masked_command):
        args = given_command.split()

        expected_command = expected_masked_command.split()

        exec_date = datetime.utcnow()
        namespace = Namespace(dag_id='foo', task_id='bar', subcommand='test', execution_date=exec_date)
        with mock.patch.object(sys, "argv", args):
            metrics = cli._build_metrics(args[1], namespace)

        self.assertTrue(metrics.get('start_datetime') <= datetime.utcnow())

        log = metrics.get('log')
        command = json.loads(log.extra).get('full_command')  # type: str
        # Replace single quotes to double quotes to avoid json decode error
        command = json.loads(command.replace("'", '"'))
        self.assertEqual(command, expected_command)

    def test_setup_locations_relative_pid_path(self):
        relative_pid_path = "fake.pid"
        pid_full_path = os.path.join(os.getcwd(), relative_pid_path)
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=relative_pid_path)
        self.assertEqual(pid, pid_full_path)

    def test_setup_locations_absolute_pid_path(self):
        abs_pid_path = os.path.join(os.getcwd(), "fake.pid")
        pid, _, _, _ = cli.setup_locations(process="fake_process", pid=abs_pid_path)
        self.assertEqual(pid, abs_pid_path)

    def test_setup_locations_none_pid_path(self):
        process_name = "fake_process"
        default_pid_path = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process_name}.pid")
        pid, _, _, _ = cli.setup_locations(process=process_name)
        self.assertEqual(pid, default_pid_path)


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
