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
import contextlib
import errno
import io

import logging
import os

from airflow.configuration import conf
from parameterized import parameterized
from six import StringIO, PY2
import sys

from datetime import datetime, timedelta, time
from mock import patch, MagicMock
from time import sleep, time as timetime
import psutil
import pytz
import subprocess
import pytest
from argparse import Namespace
from airflow import settings
import airflow.bin.cli as cli
from airflow.bin.cli import run, get_dag
from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow.utils.file import TemporaryDirectory
from airflow.utils.state import State
from airflow.settings import Session
from airflow import models
from airflow.version import version as airflow_version
from tests.compat import mock
from tests.test_utils.config import conf_vars

if PY2:
    # Need `assertWarns` back-ported from unittest2
    import unittest2 as unittest
    from backports import tempfile
else:
    import unittest
    import tempfile

if PY2:
    @contextlib.contextmanager
    def redirect_stdout(target):
        original = sys.stdout
        sys.stdout = target
        yield
        sys.stdout = original
else:
    redirect_stdout = contextlib.redirect_stdout


class ByteableIO(io.StringIO):
    def write(self, message):
        if isinstance(message, str):
            super(ByteableIO, self).write(message.decode('utf-8'))


dag_folder_path = '/'.join(os.path.realpath(__file__).split('/')[:-1])

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1))
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'


def reset(dag_id):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


def create_mock_args(
    task_id,
    dag_id,
    subdir,
    execution_date,
    task_params=None,
    dry_run=False,
    queue=None,
    pool=None,
    priority_weight_total=None,
    retries=0,
    local=True,
    mark_success=False,
    ignore_all_dependencies=False,
    ignore_depends_on_past=False,
    ignore_dependencies=False,
    force=False,
    run_as_user=None,
    executor_config=None,
    cfg_path=None,
    pickle=None,
    raw=None,
    interactive=None,
):
    if executor_config is None:
        executor_config = {}
    args = MagicMock(spec=Namespace)
    args.task_id = task_id
    args.dag_id = dag_id
    args.subdir = subdir
    args.task_params = task_params
    args.execution_date = execution_date
    args.dry_run = dry_run
    args.queue = queue
    args.pool = pool
    args.priority_weight_total = priority_weight_total
    args.retries = retries
    args.local = local
    args.run_as_user = run_as_user
    args.executor_config = executor_config
    args.cfg_path = cfg_path
    args.pickle = pickle
    args.raw = raw
    args.mark_success = mark_success
    args.ignore_all_dependencies = ignore_all_dependencies
    args.ignore_depends_on_past = ignore_depends_on_past
    args.ignore_dependencies = ignore_dependencies
    args.force = force
    args.interactive = interactive
    # Needed for CLI deprecation warning decorator
    args.subcommand = "fake-group"
    return args


class TestCLI(unittest.TestCase):

    EXAMPLE_DAGS_FOLDER = os.path.join(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(os.path.realpath(__file__))
            )
        ),
        "airflow/example_dags"
    )

    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def test_cli_webserver_debug(self):
        env = os.environ.copy()
        p = psutil.Popen(["airflow", "webserver", "-d"], env=env)
        sleep(3)  # wait for webserver to start
        return_code = p.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        p.terminate()
        p.wait()

    def test_cli_rbac_webserver_debug(self):
        env = os.environ.copy()
        env['AIRFLOW__WEBSERVER__RBAC'] = 'True'
        p = psutil.Popen(["airflow", "webserver", "-d"], env=env)
        sleep(3)  # wait for webserver to start
        return_code = p.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        p.terminate()
        p.wait()

    @pytest.mark.quarantined
    def test_local_run(self):
        args = create_mock_args(
            task_id='print_the_context',
            dag_id='example_python_operator',
            subdir='/root/dags/example_python_operator.py',
            interactive=True,
            execution_date=timezone.parse('2018-04-27T08:39:51.298439+00:00')
        )

        reset(args.dag_id)

        run(args)
        dag = get_dag(args)
        task = dag.get_task(task_id=args.task_id)
        ti = TaskInstance(task, args.execution_date)
        ti.refresh_from_db()
        state = ti.current_state()
        self.assertEqual(state, State.SUCCESS)

    def test_test(self):
        """Test the `airflow test` command"""
        args = create_mock_args(
            task_id='print_the_context',
            dag_id='example_python_operator',
            subdir=None,
            execution_date=timezone.parse('2018-01-01')
        )

        saved_stdout = sys.stdout
        try:
            sys.stdout = out = StringIO()
            cli.test(args)

            output = out.getvalue()
            # Check that prints, and log messages, are shown
            self.assertIn('END_DATE', output)
            self.assertIn("'example_python_operator__print_the_context__20180101'", output)
        finally:
            sys.stdout = saved_stdout

    @pytest.mark.quarantined
    def test_next_execution(self):
        # A scaffolding function
        def reset_dr_db(dag_id):
            session = Session()
            dr = session.query(models.DagRun).filter_by(dag_id=dag_id)
            dr.delete()
            session.commit()
            session.close()

        dag_ids = ['example_bash_operator',  # schedule_interval is '0 0 * * *'
                   'latest_only',  # schedule_interval is timedelta(hours=4)
                   'example_python_operator',  # schedule_interval=None
                   'example_xcom']  # schedule_interval="@once"

        # The details below is determined by the schedule_interval of example DAGs
        now = timezone.utcnow()
        next_execution_time_for_dag1 = pytz.utc.localize(
            datetime.combine(
                now.date() + timedelta(days=1),
                time(0)
            )
        )
        next_execution_time_for_dag2 = now + timedelta(hours=4)
        expected_output = [str(next_execution_time_for_dag1),
                           str(next_execution_time_for_dag2),
                           "None",
                           "None"]

        for i in range(len(dag_ids)):
            dag_id = dag_ids[i]

            # Clear dag run so no execution history fo each DAG
            reset_dr_db(dag_id)

            p = subprocess.Popen(["airflow", "next_execution", dag_id,
                                  "--subdir", self.EXAMPLE_DAGS_FOLDER],
                                 stdout=subprocess.PIPE)
            p.wait()
            stdout = []
            for line in p.stdout:
                stdout.append(str(line.decode("utf-8").rstrip()))

            # `next_execution` function is inapplicable if no execution record found
            # It prints `None` in such cases
            self.assertEqual(stdout[-1], "None")

            dag = self.dagbag.dags[dag_id]
            # Create a DagRun for each DAG, to prepare for next step
            dag.create_dagrun(
                run_id='manual__' + now.isoformat(),
                execution_date=now,
                start_date=now,
                state=State.FAILED
            )

            p = subprocess.Popen(["airflow", "next_execution", dag_id,
                                  "--subdir", self.EXAMPLE_DAGS_FOLDER],
                                 stdout=subprocess.PIPE)
            p.wait()
            stdout = []
            for line in p.stdout:
                stdout.append(str(line.decode("utf-8").rstrip()))
            self.assertEqual(stdout[-1], expected_output[i])

            reset_dr_db(dag_id)

    def test_generate_pod_template(self):

        from airflow.kubernetes.pod_generator import PodGenerator
        with tempfile.TemporaryDirectory("airflow_dry_run_test/") as directory:
            d = directory
            print(d)
            cli.generate_pod_template(self.parser.parse_args(
                ['generate_pod_template', '-o', directory]))
            self.assertTrue(os.path.isdir(directory))
            self.assertTrue(os.path.isfile(os.path.join(directory, 'airflow_template.yml')))

            # ensure that a valid pod is created from YAML
            result = PodGenerator.deserialize_model_file(os.path.join(directory, 'airflow_template.yml'))
            self.assertIsNotNone(result)

    @mock.patch("airflow.bin.cli.DAG.run")
    def test_backfill(self, mock_run):
        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator',
            '-s', DEFAULT_DATE.isoformat()]))

        mock_run.assert_called_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=False,
            ignore_task_deps=False,
            local=False,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
        )
        mock_run.reset_mock()
        dag = self.dagbag.get_dag('example_bash_operator')

        with self.assertLogs(level=logging.DEBUG):
            with mock.patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                cli.backfill(self.parser.parse_args([
                    'backfill', 'example_bash_operator', '-t', 'runme_0', '--dry_run',
                    '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_stdout.seek(0, 0)
        self.assertListEqual(
            [
                u"Dry run of DAG example_bash_operator on {}\n".format(DEFAULT_DATE.isoformat()),
                u"Task runme_0\n",
            ],
            mock_stdout.readlines()
        )

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.backfill(self.parser.parse_args([
            'backfill', 'example_bash_operator', '-l',
            '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_run.assert_called_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=False,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
        )
        mock_run.reset_mock()

    def test_show_dag_print(self):
        temp_stdout = ByteableIO() if PY2 else io.StringIO()
        with redirect_stdout(temp_stdout):
            cli.show_dag(self.parser.parse_args([
                'show_dag', 'example_bash_operator']))
        out = temp_stdout.getvalue()
        self.assertIn("label=example_bash_operator", out)
        self.assertIn("graph [label=example_bash_operator labelloc=t rankdir=LR]", out)
        self.assertIn("runme_2 -> run_after_loop", out)

    @mock.patch("airflow.bin.cli.render_dag")
    def test_show_dag_dave(self, mock_render_dag):
        temp_stdout = ByteableIO() if PY2 else io.StringIO()
        with redirect_stdout(temp_stdout):
            cli.show_dag(self.parser.parse_args([
                'show_dag', 'example_bash_operator', '--save', 'awesome.png']
            ))
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.render.assert_called_once_with(
            cleanup=True, filename='awesome', format='png'
        )
        self.assertIn("File awesome.png saved", out)

    @mock.patch("airflow.bin.cli.subprocess.Popen")
    @mock.patch("airflow.bin.cli.render_dag")
    def test_show_dag_imgcat(self, mock_render_dag, mock_popen):
        mock_render_dag.return_value.pipe.return_value = b"DOT_DATA"
        mock_popen.return_value.communicate.return_value = (b"OUT", b"ERR")
        temp_stdout = ByteableIO() if PY2 else io.StringIO()
        with redirect_stdout(temp_stdout):
            cli.show_dag(self.parser.parse_args([
                'show_dag', 'example_bash_operator', '--imgcat']
            ))
        mock_render_dag.return_value.pipe.assert_called_once_with(format='png')
        mock_popen.return_value.communicate.assert_called_once_with(b'DOT_DATA')

    @mock.patch("airflow.bin.cli.DAG.run")
    def test_cli_backfill_depends_on_past(self, mock_run):
        """
        Test that CLI respects -I argument

        We just check we call dag.run() right. The behaviour of that kwarg is
        tested in test_jobs
        """
        dag_id = 'test_dagrun_states_deadlock'
        run_date = DEFAULT_DATE + timedelta(days=1)
        args = [
            'backfill',
            dag_id,
            '-l',
            '-s',
            run_date.isoformat(),
            '-I',
        ]
        dag = self.dagbag.get_dag(dag_id)

        cli.backfill(self.parser.parse_args(args), dag=dag)

        mock_run.assert_called_with(
            start_date=run_date,
            end_date=run_date,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=False,
            verbose=False,
        )

    @mock.patch("airflow.bin.cli.DAG.run")
    def test_cli_backfill_depends_on_past_backwards(self, mock_run):
        """
        Test that CLI respects -B argument and raises on interaction with depends_on_past
        """
        dag_id = 'test_depends_on_past'
        start_date = DEFAULT_DATE + timedelta(days=1)
        end_date = start_date + timedelta(days=1)
        args = [
            'backfill',
            dag_id,
            '-l',
            '-s',
            start_date.isoformat(),
            '-e',
            end_date.isoformat(),
            '-I',
            '-B',
        ]
        dag = self.dagbag.get_dag(dag_id)

        cli.backfill(self.parser.parse_args(args), dag=dag)
        mock_run.assert_called_with(
            start_date=start_date,
            end_date=end_date,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
            ignore_task_deps=False,
            local=True,
            mark_success=False,
            pool=None,
            rerun_failed_tasks=False,
            run_backwards=True,
            verbose=False,
        )

    @mock.patch("airflow.bin.cli.jobs.LocalTaskJob")
    def test_run_naive_taskinstance(self, mock_local_job):
        """
        Test that we can run naive (non-localized) task instances
        """
        NAIVE_DATE = datetime(2016, 1, 1)
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')

        task0_id = 'test_run_dependent_task'
        args0 = ['run',
                 '-A',
                 '--local',
                 dag_id,
                 task0_id,
                 NAIVE_DATE.isoformat()]

        cli.run(self.parser.parse_args(args0), dag=dag)
        mock_local_job.assert_called_with(
            task_instance=mock.ANY,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pickle_id=None,
            pool=None,
        )


class TestLogsfromTaskRunCommand(unittest.TestCase):

    def setUp(self):
        self.dag_id = "test_logging_dag"
        self.task_id = "test_task"
        reset(self.dag_id)
        self.execution_date_str = timezone.make_aware(datetime(2017, 1, 1)).isoformat()
        self.log_dir = conf.get('core', 'base_log_folder')
        self.log_filename = "{}/{}/{}/1.log".format(self.dag_id, self.task_id, self.execution_date_str)
        self.ti_log_file_path = os.path.join(self.log_dir, self.log_filename)
        self.parser = cli.CLIFactory.get_parser()
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def tearDown(self):
        reset(self.dag_id)
        try:
            os.remove(self.ti_log_file_path)
        except OSError:
            pass

    def assert_log_line(self, text, logs_list, expect_from_logging_mixin=False):
        """
        Get Log Line and assert only 1 Entry exists with the given text. Also check that
        "logging_mixin" line does not appear in that log line to avoid duplicate loggigng as below:
        [2020-06-24 16:47:23,537] {logging_mixin.py:91} INFO - [2020-06-24 16:47:23,536] {python.py:135}
        """
        log_lines = [log for log in logs_list if text in log]
        self.assertEqual(len(log_lines), 1)
        log_line = log_lines[0]
        if not expect_from_logging_mixin:
            # Logs from print statement still show with logging_mixing as filename
            # Example: [2020-06-24 17:07:00,482] {logging_mixin.py:91} INFO - Log from Print statement
            self.assertNotIn("logging_mixin.py", log_line)
        return log_line

    @unittest.skipIf(not hasattr(os, 'fork'), "Forking not available")
    def test_logging_with_run_task(self):
        #  We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed

        with conf_vars({('core', 'dags_folder'): os.path.join(TEST_DAG_FOLDER, self.dag_id)}):
            cli.run(self.parser.parse_args([
                'run', self.dag_id, self.task_id, '--local', self.execution_date_str]))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)     # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        self.assertIn("INFO - Started process", logs)
        self.assertIn("Subtask {}".format(self.task_id), logs)
        self.assertIn("standard_task_runner.py", logs)
        self.assertIn("INFO - Running: ['airflow', 'run', '{}', "
                      "'{}', '{}',".format(self.dag_id, self.task_id, self.execution_date_str), logs)

        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        self.assertIn("INFO - Marking task as SUCCESS.dag_id={}, task_id={}, "
                      "execution_date=20170101T000000".format(self.dag_id, self.task_id), logs)

    @mock.patch("airflow.task.task_runner.standard_task_runner.CAN_FORK", False)
    def test_logging_with_run_task_subprocess(self):
        # We are not using self.assertLogs as we want to verify what actually is stored in the Log file
        # as that is what gets displayed
        with conf_vars({('core', 'dags_folder'): os.path.join(TEST_DAG_FOLDER, self.dag_id)}):
            cli.run(self.parser.parse_args([
                'run', self.dag_id, self.task_id, '--local', self.execution_date_str]))

        with open(self.ti_log_file_path) as l_file:
            logs = l_file.read()

        print(logs)     # In case of a test failures this line would show detailed log
        logs_list = logs.splitlines()

        self.assertIn("Subtask {}".format(self.task_id), logs)
        self.assertIn("base_task_runner.py", logs)
        self.assert_log_line("Log from DAG Logger", logs_list)
        self.assert_log_line("Log from TI Logger", logs_list)
        self.assert_log_line("Log from Print statement", logs_list, expect_from_logging_mixin=True)

        self.assertIn("INFO - Running: ['airflow', 'run', '{}', "
                      "'{}', '{}',".format(self.dag_id, self.task_id, self.execution_date_str), logs)
        self.assertIn("INFO - Marking task as SUCCESS.dag_id={}, task_id={}, "
                      "execution_date=20170101T000000".format(self.dag_id, self.task_id), logs)


@pytest.mark.integration("redis")
@pytest.mark.integration("rabbitmq")
class TestWorkerServeLogs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch('celery.bin.worker.worker')
    def test_serve_logs_on_worker_start(self, celery_mock):
        with patch('airflow.bin.cli.subprocess.Popen') as mock_popen:
            mock_popen.return_value.communicate.return_value = (b'output', b'error')
            mock_popen.return_value.returncode = 0
            args = self.parser.parse_args(['worker', '-c', '-1'])

            with patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                cli.worker(args)
                mock_popen.assert_called()

    @mock.patch('celery.bin.worker.worker')
    def test_skip_serve_logs_on_worker_start(self, celery_mock):
        with patch('airflow.bin.cli.subprocess.Popen') as mock_popen:
            mock_popen.return_value.communicate.return_value = (b'output', b'error')
            mock_popen.return_value.returncode = 0
            args = self.parser.parse_args(['worker', '-c', '-1', '-s'])

            with patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                cli.worker(args)
                mock_popen.assert_not_called()


class TestWorkerStart(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch('airflow.bin.cli.setup_logging')
    @mock.patch('celery.bin.worker.worker')
    def test_worker_started_with_required_arguments(self, mock_worker, setup_logging_mock):
        concurrency = '1'
        celery_hostname = "celery_hostname"
        queues = "queue"
        autoscale = "2,5"
        args = self.parser.parse_args([
            'worker',
            '--autoscale',
            autoscale,
            '--concurrency',
            concurrency,
            '--celery_hostname',
            celery_hostname,
            '--queues',
            queues
        ])

        with mock.patch('celery.platforms.check_privileges') as mock_privil:
            mock_privil.return_value = 0
            cli.worker(args)

        mock_worker.return_value.run.assert_called_once_with(
            pool='prefork',
            optimization='fair',
            O='fair',  # noqa
            queues=queues,
            concurrency=int(concurrency),
            autoscale=autoscale,
            hostname=celery_hostname,
            loglevel=mock.ANY,
        )


class TestCliConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.bin.cli.cli_utils.should_use_colors", return_value=False)
    @mock.patch("airflow.bin.cli.io.StringIO")
    @mock.patch("airflow.bin.cli.conf")
    def test_cli_show_config_should_write_data(self, mock_conf, mock_stringio, mock_should_use_colors):
        cli.config(self.parser.parse_args(['config']))
        mock_conf.write.assert_called_once_with(mock_stringio.return_value.__enter__.return_value)

    @conf_vars({
        ('core', 'testkey'): 'test_value'
    })
    def test_cli_show_config_should_display_key(self):
        temp_stdout = StringIO()
        with mock.patch("sys.stdout", temp_stdout):
            cli.config(self.parser.parse_args(['config', '--color=off']))
        self.assertIn('[core]', temp_stdout.getvalue())
        self.assertIn('testkey = test_value', temp_stdout.getvalue())


class TestPiiAnonymizer(unittest.TestCase):
    def setUp(self):
        self.instance = cli.PiiAnonymizer()

    def test_should_remove_pii_from_path(self):
        home_path = os.path.expanduser("~/airflow/config")
        self.assertEqual("${HOME}/airflow/config", self.instance.process_path(home_path))

    @parameterized.expand(
        [
            (
                "postgresql+psycopg2://postgres:airflow@postgres/airflow",
                "postgresql+psycopg2://p...s:PASSWORD@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://postgres@postgres/airflow",
                "postgresql+psycopg2://p...s@postgres/airflow",
            ),
            (
                "postgresql+psycopg2://:airflow@postgres/airflow",
                "postgresql+psycopg2://:PASSWORD@postgres/airflow",
            ),
            ("postgresql+psycopg2://postgres/airflow", "postgresql+psycopg2://postgres/airflow",),
        ]
    )
    def test_should_remove_pii_from_url(self, before, after):
        self.assertEqual(after, self.instance.process_url(before))


class TestAirflowInfo(unittest.TestCase):
    def test_should_be_string(self):
        text = str(cli.AirflowInfo(cli.NullAnonymizer()))

        self.assertIn("Apache Airflow [{}]".format(airflow_version), text)


class TestSystemInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(cli.SystemInfo(cli.NullAnonymizer())))


class TestPathsInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(cli.PathsInfo(cli.NullAnonymizer())))


class TestConfigInfo(unittest.TestCase):
    @conf_vars(
        {
            ("core", "executor"): "TEST_EXECUTOR",
            ("core", "dags_folder"): "TEST_DAGS_FOLDER",
            ("core", "plugins_folder"): "TEST_PLUGINS_FOLDER",
            ("core", "base_log_folder"): "TEST_LOG_FOLDER",
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_should_read_config(self):
        instance = cli.ConfigInfo(cli.NullAnonymizer())
        text = str(instance)
        self.assertIn("TEST_EXECUTOR", text)
        self.assertIn("TEST_DAGS_FOLDER", text)
        self.assertIn("TEST_PLUGINS_FOLDER", text)
        self.assertIn("TEST_LOG_FOLDER", text)
        self.assertIn("postgresql+psycopg2://postgres:airflow@postgres/airflow", text)


class TestToolsInfo(unittest.TestCase):
    def test_should_be_string(self):
        self.assertTrue(str(cli.ToolsInfo(cli.NullAnonymizer())))


class TestShowInfo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.get_parser()

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info(self):
        temp_stdout = StringIO()
        with mock.patch("sys.stdout", temp_stdout):
            cli.info(self.parser.parse_args(["info"]))

        output = temp_stdout.getvalue()
        self.assertIn("Apache Airflow [{}]".format(airflow_version), output)
        self.assertIn("postgresql+psycopg2://postgres:airflow@postgres/airflow", output)

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    def test_show_info_anonymize(self):
        temp_stdout = StringIO()
        with mock.patch("sys.stdout", temp_stdout):
            cli.info(self.parser.parse_args(["info", "--anonymize"]))

        output = temp_stdout.getvalue()
        self.assertIn("Apache Airflow [{}]".format(airflow_version), output)
        self.assertIn("postgresql+psycopg2://p...s:PASSWORD@postgres/airflow", output)

    @conf_vars(
        {
            ('core', 'sql_alchemy_conn'): 'postgresql+psycopg2://postgres:airflow@postgres/airflow',
        }
    )
    @mock.patch(  # type: ignore
        "airflow.bin.cli.requests",
        **{
            "post.return_value.ok": True,
            "post.return_value.json.return_value": {
                "success": True,
                "key": "f9U3zs3I",
                "link": "https://file.io/TEST",
                "expiry": "14 days",
            },
        }
    )
    def test_show_info_anonymize_fileio(self, mock_requests):
        temp_stdout = StringIO()
        with mock.patch("sys.stdout", temp_stdout):
            cli.info(self.parser.parse_args(["info", "--file-io"]))

        self.assertIn("https://file.io/TEST", temp_stdout.getvalue())
        content = mock_requests.post.call_args[1]["files"]["file"][1]
        self.assertIn("postgresql+psycopg2://p...s:PASSWORD@postgres/airflow", content)


class TestGunicornMonitor(unittest.TestCase):

    def setUp(self):
        self.monitor = cli.GunicornMonitor(
            gunicorn_master_pid=1,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )
        mock.patch.object(self.monitor, '_generate_plugin_state', return_value={}).start()
        mock.patch.object(self.monitor, '_get_num_ready_workers_running', return_value=4).start()
        mock.patch.object(self.monitor, '_get_num_workers_running', return_value=4).start()
        mock.patch.object(self.monitor, '_spawn_new_workers', return_value=None).start()
        mock.patch.object(self.monitor, '_kill_old_workers', return_value=None).start()
        mock.patch.object(self.monitor, '_reload_gunicorn', return_value=None).start()

    @mock.patch('airflow.bin.cli.time.sleep')
    def test_should_wait_for_workers_to_start(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 0
        self.monitor._get_num_workers_running.return_value = 4
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.bin.cli.time.sleep')
    def test_should_kill_excess_workers(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 10
        self.monitor._get_num_workers_running.return_value = 10
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.bin.cli.time.sleep')
    def test_should_start_new_workers_when_missing(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 2
        self.monitor._get_num_workers_running.return_value = 2
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.bin.cli.time.sleep')
    def test_should_start_new_workers_when_refresh_interval_has_passed(self, mock_sleep):
        self.monitor._last_refresh_time -= 200
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member
        self.assertAlmostEqual(self.monitor._last_refresh_time, timetime(), delta=5)

    @mock.patch('airflow.bin.cli.time.sleep')
    def test_should_reload_when_plugin_has_been_changed(self, mock_sleep):
        self.monitor._generate_plugin_state.return_value = {'AA': 12}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

        self.monitor._generate_plugin_state.return_value = {'AA': 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

        self.monitor._generate_plugin_state.return_value = {'AA': 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_called_once_with()  # pylint: disable=no-member
        self.assertAlmostEqual(self.monitor._last_refresh_time, timetime(), delta=5)


class TestGunicornMonitorGeneratePluginState(unittest.TestCase):
    @staticmethod
    def _prepare_test_file(filepath, size):
        try:
            os.makedirs(os.path.dirname(filepath))
        except OSError as e:
            # be happy if someone already created the path
            if e.errno != errno.EEXIST:
                raise
        with open(filepath, "w") as file:
            file.write("A" * size)
            file.flush()

    def test_should_detect_changes_in_directory(self):
        with TemporaryDirectory(prefix="tmp") as tempdir, \
                mock.patch("airflow.bin.cli.settings.PLUGINS_FOLDER", tempdir):
            self._prepare_test_file("{}/file1.txt".format(tempdir), 100)
            self._prepare_test_file("{}/nested/nested/nested/nested/file2.txt".format(tempdir), 200)
            self._prepare_test_file("{}/file3.txt".format(tempdir), 300)

            monitor = cli.GunicornMonitor(
                gunicorn_master_pid=1,
                num_workers_expected=4,
                master_timeout=60,
                worker_refresh_interval=60,
                worker_refresh_batch_size=2,
                reload_on_plugin_change=True,
            )

            # When the files have not changed, the result should be constant
            state_a = monitor._generate_plugin_state()
            state_b = monitor._generate_plugin_state()

            self.assertEqual(state_a, state_b)
            self.assertEqual(3, len(state_a))

            # Should detect new file
            self._prepare_test_file("{}/file4.txt".format(tempdir), 400)

            state_c = monitor._generate_plugin_state()

            self.assertNotEqual(state_b, state_c)
            self.assertEqual(4, len(state_c))

            # Should detect changes in files
            self._prepare_test_file("{}/file4.txt".format(tempdir), 450)

            state_d = monitor._generate_plugin_state()

            self.assertNotEqual(state_c, state_d)
            self.assertEqual(4, len(state_d))

            # Should support large files
            self._prepare_test_file("{}/file4.txt".format(tempdir), 4000000)

            state_d = monitor._generate_plugin_state()

            self.assertNotEqual(state_c, state_d)
            self.assertEqual(4, len(state_d))


class TestCLIGetNumReadyWorkersRunning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.parser = cli.get_parser()

    def setUp(self):
        self.gunicorn_master_proc = mock.Mock(pid=2137)
        self.children = mock.MagicMock()
        self.child = mock.MagicMock()
        self.process = mock.MagicMock()
        self.monitor = cli.GunicornMonitor(
            gunicorn_master_pid=1,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 1)

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)
