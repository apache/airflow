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
import io
import os
import re
import subprocess
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, time, timedelta
from time import sleep
from unittest.mock import MagicMock, Mock, patch

import psutil
import pytz

import airflow.bin.cli as cli
from airflow import AirflowException, models, settings
from airflow.bin.cli import get_num_ready_workers_running
from airflow.models import Connection, DagModel, Variable
from airflow.settings import Session
from airflow.utils import db, timezone
from airflow.utils.state import State
from airflow.version import version
from tests import conf_vars
from tests.compat import mock

dag_folder_path = '/'.join(os.path.realpath(__file__).split('/')[:-1])

DEV_NULL = "/dev/null"
DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1))
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'


def create_mock_args(  # pylint: disable=too-many-arguments
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
    return args


EXAMPLE_DAGS_FOLDER = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))
        )
    ),
    "airflow/example_dags"
)


class TestCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self):
        self.gunicorn_master_proc = Mock(pid=None)
        self.children = MagicMock()
        self.child = MagicMock()
        self.process = MagicMock()

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 1)

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_cli_webserver_debug(self):
        env = os.environ.copy()
        proc = psutil.Popen(["airflow", "webserver", "-d"], env=env)
        sleep(3)  # wait for webserver to start
        return_code = proc.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        proc.terminate()
        proc.wait()


class TestCliDags(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.bin.cli.DAG.run")
    def test_backfill(self, mock_run):
        cli.dag_backfill(self.parser.parse_args([
            'dags', 'backfill', 'example_bash_operator',
            '-s', DEFAULT_DATE.isoformat()]))

        mock_run.assert_called_once_with(
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

        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.dag_backfill(self.parser.parse_args([
                'dags', 'backfill', 'example_bash_operator', '-t', 'runme_0', '--dry_run',
                '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_stdout.seek(0, 0)

        output = mock_stdout.read()
        self.assertIn("Dry run of DAG example_bash_operator on {}\n".format(DEFAULT_DATE.isoformat()), output)
        self.assertIn("Task runme_0\n".format(DEFAULT_DATE.isoformat()), output)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.dag_backfill(self.parser.parse_args([
            'dags', 'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.dag_backfill(self.parser.parse_args([
            'dags', 'backfill', 'example_bash_operator', '-l',
            '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_run.assert_called_once_with(
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
        temp_stdout = io.StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            cli.dag_show(self.parser.parse_args([
                'dags', 'show', 'example_bash_operator']))
        out = temp_stdout.getvalue()
        self.assertIn("label=example_bash_operator", out)
        self.assertIn("graph [label=example_bash_operator labelloc=t rankdir=LR]", out)
        self.assertIn("runme_2 -> run_after_loop", out)

    @mock.patch("airflow.bin.cli.render_dag")
    def test_show_dag_dave(self, mock_render_dag):
        temp_stdout = io.StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            cli.dag_show(self.parser.parse_args([
                'dags', 'show', 'example_bash_operator', '--save', 'awesome.png']
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
        temp_stdout = io.StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            cli.dag_show(self.parser.parse_args([
                'dags', 'show', 'example_bash_operator', '--imgcat']
            ))
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.pipe.assert_called_once_with(format='png')
        mock_popen.return_value.communicate.assert_called_once_with(b'DOT_DATA')
        self.assertIn("OUT", out)
        self.assertIn("ERR", out)

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
            'dags',
            'backfill',
            dag_id,
            '-l',
            '-s',
            run_date.isoformat(),
            '-I',
        ]
        dag = self.dagbag.get_dag(dag_id)

        cli.dag_backfill(self.parser.parse_args(args), dag=dag)

        mock_run.assert_called_once_with(
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
            'dags',
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

        cli.dag_backfill(self.parser.parse_args(args), dag=dag)
        mock_run.assert_called_once_with(
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

        for i in range(len(dag_ids)):  # pylint: disable=consider-using-enumerate
            dag_id = dag_ids[i]

            # Clear dag run so no execution history fo each DAG
            reset_dr_db(dag_id)

            proc = subprocess.Popen(["airflow", "dags", "next_execution", dag_id,
                                     "--subdir", EXAMPLE_DAGS_FOLDER],
                                    stdout=subprocess.PIPE)
            proc.wait()
            stdout = []
            for line in proc.stdout:
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

            proc = subprocess.Popen(["airflow", "dags", "next_execution", dag_id,
                                     "--subdir", EXAMPLE_DAGS_FOLDER],
                                    stdout=subprocess.PIPE)
            proc.wait()
            stdout = []
            for line in proc.stdout:
                stdout.append(str(line.decode("utf-8").rstrip()))
            self.assertEqual(stdout[-1], expected_output[i])

            reset_dr_db(dag_id)

    def test_cli_list_dags(self):
        args = self.parser.parse_args(['dags', 'list', '--report'])
        cli.dag_list_dags(args)

    def test_cli_list_dag_runs(self):
        cli.dag_trigger(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator', ]))
        args = self.parser.parse_args(['dags', 'list_runs',
                                       'example_bash_operator',
                                       '--no_backfill'])
        cli.dag_list_dag_runs(args)

    def test_cli_list_jobs_with_args(self):
        args = self.parser.parse_args(['dags', 'list_jobs', '--dag_id',
                                       'example_bash_operator',
                                       '--state', 'success',
                                       '--limit', '100',
                                       '--output', 'tsv'])
        cli.dag_list_jobs(args)

    def test_pause(self):
        args = self.parser.parse_args([
            'dags', 'pause', 'example_bash_operator'])
        cli.dag_pause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [True, 1])

        args = self.parser.parse_args([
            'dags', 'unpause', 'example_bash_operator'])
        cli.dag_unpause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [False, 0])

    def test_trigger_dag(self):
        cli.dag_trigger(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator',
            '-c', '{"foo": "bar"}']))
        self.assertRaises(
            ValueError,
            cli.dag_trigger,
            self.parser.parse_args([
                'dags', 'trigger', 'example_bash_operator',
                '--run_id', 'trigger_dag_xxx',
                '-c', 'NOT JSON'])
        )

    def test_delete_dag(self):
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key))
        session.commit()
        cli.dag_delete(self.parser.parse_args([
            'dags', 'delete', key, '--yes']))
        self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)
        self.assertRaises(
            AirflowException,
            cli.dag_delete,
            self.parser.parse_args([
                'dags', 'delete',
                'does_not_exist_dag',
                '--yes'])
        )

    def test_delete_dag_existing_file(self):
        # Test to check that the DAG should be deleted even if
        # the file containing it is not deleted
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        with tempfile.NamedTemporaryFile() as f:
            session.add(DM(dag_id=key, fileloc=f.name))
            session.commit()
            cli.dag_delete(self.parser.parse_args([
                'dags', 'delete', key, '--yes']))
            self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)

    def test_cli_list_jobs(self):
        args = self.parser.parse_args(['dags', 'list_jobs'])
        cli.dag_list_jobs(args)

    def test_dag_state(self):
        self.assertEqual(None, cli.dag_state(self.parser.parse_args([
            'dags', 'state', 'example_bash_operator', DEFAULT_DATE.isoformat()])))


class TestCliVariables(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def test_variables(self):
        # Checks if all subcommands are properly received
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"bar"}']))
        cli.variables_get(self.parser.parse_args([
            'variables', 'get', 'foo']))
        cli.variables_get(self.parser.parse_args([
            'variables', 'get', 'baz', '-d', 'bar']))
        cli.variables_list(self.parser.parse_args([
            'variables', 'list']))
        cli.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'bar']))
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', DEV_NULL]))
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', DEV_NULL]))

        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'original']))
        # First export
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables1.json']))

        first_exp = open('variables1.json', 'r')

        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'bar', 'updated']))
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'foo', '{"foo":"oops"}']))
        cli.variables_delete(self.parser.parse_args([
            'variables', 'delete', 'foo']))
        # First import
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables1.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))
        # Second export
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables2.json']))

        second_exp = open('variables2.json', 'r')
        self.assertEqual(first_exp.read(), second_exp.read())
        second_exp.close()
        first_exp.close()
        # Second import
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables2.json']))

        self.assertEqual('original', Variable.get('bar'))
        self.assertEqual('{\n  "foo": "bar"\n}', Variable.get('foo'))

        # Set a dict
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'dict', '{"foo": "oops"}']))
        # Set a list
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'list', '["oops"]']))
        # Set str
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'str', 'hello string']))
        # Set int
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'int', '42']))
        # Set float
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'float', '42.0']))
        # Set true
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'true', 'true']))
        # Set false
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'false', 'false']))
        # Set none
        cli.variables_set(self.parser.parse_args([
            'variables', 'set', 'null', 'null']))

        # Export and then import
        cli.variables_export(self.parser.parse_args([
            'variables', 'export', 'variables3.json']))
        cli.variables_import(self.parser.parse_args([
            'variables', 'import', 'variables3.json']))

        # Assert value
        self.assertEqual({'foo': 'oops'}, Variable.get('dict', deserialize_json=True))
        self.assertEqual(['oops'], Variable.get('list', deserialize_json=True))
        self.assertEqual('hello string', Variable.get('str'))  # cannot json.loads(str)
        self.assertEqual(42, Variable.get('int', deserialize_json=True))
        self.assertEqual(42.0, Variable.get('float', deserialize_json=True))
        self.assertEqual(True, Variable.get('true', deserialize_json=True))
        self.assertEqual(False, Variable.get('false', deserialize_json=True))
        self.assertEqual(None, Variable.get('null', deserialize_json=True))

        os.remove('variables1.json')
        os.remove('variables2.json')
        os.remove('variables3.json')


class TestCliWebServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self) -> None:
        self._check_processes()
        self._clean_pidfiles()

    def _check_processes(self):
        try:
            # Confirm that webserver hasn't been launched.
            # pgrep returns exit status 1 if no process matched.
            self.assertEqual(1, subprocess.Popen(["pgrep", "-f", "-c", "airflow webserver"]).wait())
            self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())
        except:  # noqa: E722
            subprocess.Popen(["ps", "-ax"]).wait()
            raise

    def tearDown(self) -> None:
        self._check_processes()

    def _clean_pidfiles(self):
        pidfile_webserver = cli.setup_locations("webserver")[0]
        pidfile_monitor = cli.setup_locations("webserver-monitor")[0]
        if os.path.exists(pidfile_webserver):
            os.remove(pidfile_webserver)
        if os.path.exists(pidfile_monitor):
            os.remove(pidfile_monitor)

    def _wait_pidfile(self, pidfile):
        while True:
            try:
                with open(pidfile) as file:
                    return int(file.read())
            except Exception:  # pylint: disable=broad-except
                sleep(1)

    def test_cli_webserver_foreground(self):
        # Run webserver in foreground and terminate it.
        proc = subprocess.Popen(["airflow", "webserver"])
        proc.terminate()
        proc.wait()

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_foreground_with_pid(self):
        # Run webserver in foreground with --pid option
        pidfile = tempfile.mkstemp()[1]
        proc = subprocess.Popen(["airflow", "webserver", "--pid", pidfile])

        # Check the file specified by --pid option exists
        self._wait_pidfile(pidfile)

        # Terminate webserver
        proc.terminate()
        proc.wait()

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_background(self):
        pidfile_webserver = cli.setup_locations("webserver")[0]
        pidfile_monitor = cli.setup_locations("webserver-monitor")[0]

        # Run webserver as daemon in background. Note that the wait method is not called.
        subprocess.Popen(["airflow", "webserver", "-D"])

        pid_monitor = self._wait_pidfile(pidfile_monitor)
        self._wait_pidfile(pidfile_webserver)

        # Assert that gunicorn and its monitor are launched.
        self.assertEqual(0, subprocess.Popen(["pgrep", "-f", "-c", "airflow webserver"]).wait())
        self.assertEqual(0, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

        # Terminate monitor process.
        proc = psutil.Process(pid_monitor)
        proc.terminate()
        proc.wait()

    # Patch for causing webserver timeout
    @mock.patch("airflow.bin.cli.get_num_workers_running", return_value=0)
    def test_cli_webserver_shutdown_when_gunicorn_master_is_killed(self, _):
        # Shorten timeout so that this test doesn't take too long time
        args = self.parser.parse_args(['webserver'])
        with conf_vars({('webserver', 'web_server_master_timeout'): '10'}):
            with self.assertRaises(SystemExit) as e:
                cli.webserver(args)
        self.assertEqual(e.exception.code, 1)


class TestCliDb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.bin.cli.db.initdb")
    def test_cli_initdb(self, initdb_mock):
        cli.initdb(self.parser.parse_args(['db', 'init']))

        initdb_mock.assert_called_once_with()

    @mock.patch("airflow.bin.cli.db.resetdb")
    def test_cli_resetdb(self, resetdb_mock):
        cli.resetdb(self.parser.parse_args(['db', 'reset', '--yes']))

        resetdb_mock.assert_called_once_with()


class TestCliConnections(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    def test_cli_connections_list(self):
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_list(self.parser.parse_args(['connections', 'list']))
            stdout = mock_stdout.getvalue()
        conns = [[x.strip("'") for x in re.findall(r"'\w+'", line)[:2]]
                 for ii, line in enumerate(stdout.split('\n'))
                 if ii % 2 == 1]
        conns = [conn for conn in conns if len(conn) > 0]

        # Assert that some of the connections are present in the output as
        # expected:
        self.assertIn(['aws_default', 'aws'], conns)
        self.assertIn(['hive_cli_default', 'hive_cli'], conns)
        self.assertIn(['emr_default', 'emr'], conns)
        self.assertIn(['mssql_default', 'mssql'], conns)
        self.assertIn(['mysql_default', 'mysql'], conns)
        self.assertIn(['postgres_default', 'postgres'], conns)
        self.assertIn(['wasb_default', 'wasb'], conns)
        self.assertIn(['segment_default', 'segment'], conns)

    def test_cli_connections_list_with_args(self):
        args = self.parser.parse_args(['connections', 'list',
                                       '--output', 'tsv'])
        cli.connections_list(args)

    def test_cli_connections_list_redirect(self):
        cmd = ['airflow', 'connections', 'list']
        with tempfile.TemporaryFile() as file:
            proc = subprocess.Popen(cmd, stdout=file)
            proc.wait()
            self.assertEqual(0, proc.returncode)

    def test_cli_connections_add_delete(self):
        # TODO: We should not delete the entire database, but only reset the contents of the Connection table.
        db.resetdb()
        # Add connections:
        uri = 'postgresql://airflow:airflow@host:5432/airflow'
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new2',
                 '--conn_uri=%s' % uri]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new3',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new4',
                 '--conn_uri=%s' % uri, '--conn_extra', "{'extra': 'yes'}"]))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new5',
                 '--conn_type=hive_metastore', '--conn_login=airflow',
                 '--conn_password=airflow', '--conn_host=host',
                 '--conn_port=9083', '--conn_schema=airflow']))
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new6',
                 '--conn_uri', "", '--conn_type=google_cloud_platform', '--conn_extra', "{'extra': 'yes'}"]))
            stdout = mock_stdout.getvalue()

        # Check addition stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            ("\tSuccessfully added `conn_id`=new1 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new2 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new3 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new4 : " +
             "postgresql://airflow:airflow@host:5432/airflow"),
            ("\tSuccessfully added `conn_id`=new5 : " +
             "hive_metastore://airflow:airflow@host:9083/airflow"),
            ("\tSuccessfully added `conn_id`=new6 : " +
             "google_cloud_platform://:@:")
        ])

        # Attempt to add duplicate
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new1',
                 '--conn_uri=%s' % uri]))
            stdout = mock_stdout.getvalue()

        # Check stdout for addition attempt
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tA connection with `conn_id`=new1 already exists",
        ])

        # Attempt to add without providing conn_uri
        with self.assertRaises(SystemExit) as exc:
            cli.connections_add(self.parser.parse_args(
                ['connections', 'add', 'new']))

        self.assertEqual(
            exc.exception.code,
            "The following args are required to add a connection: ['conn_uri or conn_type']"
        )

        # Prepare to add connections
        session = settings.Session()
        extra = {'new1': None,
                 'new2': None,
                 'new3': "{'extra': 'yes'}",
                 'new4': "{'extra': 'yes'}"}

        # Add connections
        for index in range(1, 6):
            conn_id = 'new%s' % index
            result = (session
                      .query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())
            result = (result.conn_id, result.conn_type, result.host,
                      result.port, result.get_extra())
            if conn_id in ['new1', 'new2', 'new3', 'new4']:
                self.assertEqual(result, (conn_id, 'postgres', 'host', 5432,
                                          extra[conn_id]))
            elif conn_id == 'new5':
                self.assertEqual(result, (conn_id, 'hive_metastore', 'host',
                                          9083, None))
            elif conn_id == 'new6':
                self.assertEqual(result, (conn_id, 'google_cloud_platform',
                                          None, None, "{'extra': 'yes'}"))

        # Delete connections
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new1']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new2']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new3']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new4']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new5']))
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'new6']))
            stdout = mock_stdout.getvalue()

        # Check deletion stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tSuccessfully deleted `conn_id`=new1",
            "\tSuccessfully deleted `conn_id`=new2",
            "\tSuccessfully deleted `conn_id`=new3",
            "\tSuccessfully deleted `conn_id`=new4",
            "\tSuccessfully deleted `conn_id`=new5",
            "\tSuccessfully deleted `conn_id`=new6"
        ])

        # Check deletions
        for index in range(1, 7):
            conn_id = 'new%s' % index
            result = (session.query(Connection)
                      .filter(Connection.conn_id == conn_id)
                      .first())

            self.assertTrue(result is None)

        # Attempt to delete a non-existing connection
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.connections_delete(self.parser.parse_args(
                ['connections', 'delete', 'fake']))
            stdout = mock_stdout.getvalue()

        # Check deletion attempt stdout
        lines = [l for l in stdout.split('\n') if len(l) > 0]
        self.assertListEqual(lines, [
            "\tDid not find a connection with `conn_id`=fake",
        ])

        session.close()


class TestCliVersion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    def test_cli_version(self):
        with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
            cli.version(self.parser.parse_args(['version']))
            stdout = mock_stdout.getvalue()
        self.assertIn(version, stdout)
