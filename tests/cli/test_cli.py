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
import subprocess
import sys
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
from airflow.bin.cli import get_dag, get_num_ready_workers_running, run
from airflow.models import DagModel, TaskInstance
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.state import State
from tests.compat import mock

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

    def test_local_run(self):
        args = create_mock_args(
            task_id='print_the_context',
            dag_id='example_python_operator',
            subdir='/root/dags/example_python_operator.py',
            interactive=True,
            execution_date=timezone.parse('2018-04-27T08:39:51.298439+00:00')
        )

        reset(args.dag_id)

        with patch('argparse.Namespace', args) as mock_args:
            run(mock_args)
            dag = get_dag(mock_args)
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
            sys.stdout = out = io.StringIO()
            cli.test(args)

            output = out.getvalue()
            # Check that prints, and log messages, are shown
            self.assertIn("'example_python_operator__print_the_context__20180101'", output)
        finally:
            sys.stdout = saved_stdout

    @mock.patch("airflow.bin.cli.jobs.LocalTaskJob")
    def test_run_naive_taskinstance(self, mock_local_job):
        """
        Test that we can run naive (non-localized) task instances
        """
        naive_date = datetime(2016, 1, 1)
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')

        task0_id = 'test_run_dependent_task'
        args0 = ['tasks',
                 'run',
                 '-A',
                 '--local',
                 dag_id,
                 task0_id,
                 naive_date.isoformat()]

        cli.run(self.parser.parse_args(args0), dag=dag)
        mock_local_job.assert_called_once_with(
            task_instance=mock.ANY,
            mark_success=False,
            ignore_all_deps=True,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pickle_id=None,
            pool=None,
        )


class TestCliDags(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    @mock.patch("airflow.bin.cli.DAG.run")
    def test_backfill(self, mock_run):
        cli.backfill(self.parser.parse_args([
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
            cli.backfill(self.parser.parse_args([
                'dags', 'backfill', 'example_bash_operator', '-t', 'runme_0', '--dry_run',
                '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_stdout.seek(0, 0)

        output = mock_stdout.read()
        self.assertIn("Dry run of DAG example_bash_operator on {}\n".format(DEFAULT_DATE.isoformat()), output)
        self.assertIn("Task runme_0\n".format(DEFAULT_DATE.isoformat()), output)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.backfill(self.parser.parse_args([
            'dags', 'backfill', 'example_bash_operator', '--dry_run',
            '-s', DEFAULT_DATE.isoformat()]), dag=dag)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        cli.backfill(self.parser.parse_args([
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
            cli.show_dag(self.parser.parse_args([
                'dags', 'show', 'example_bash_operator']))
        out = temp_stdout.getvalue()
        self.assertIn("label=example_bash_operator", out)
        self.assertIn("graph [label=example_bash_operator labelloc=t rankdir=LR]", out)
        self.assertIn("runme_2 -> run_after_loop", out)

    @mock.patch("airflow.bin.cli.render_dag")
    def test_show_dag_dave(self, mock_render_dag):
        temp_stdout = io.StringIO()
        with contextlib.redirect_stdout(temp_stdout):
            cli.show_dag(self.parser.parse_args([
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
            cli.show_dag(self.parser.parse_args([
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

        cli.backfill(self.parser.parse_args(args), dag=dag)

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

        cli.backfill(self.parser.parse_args(args), dag=dag)
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
        cli.list_dags(args)

    def test_cli_list_dag_runs(self):
        cli.trigger_dag(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator', ]))
        args = self.parser.parse_args(['dags', 'list_runs',
                                       'example_bash_operator',
                                       '--no_backfill'])
        cli.list_dag_runs(args)

    def test_cli_list_jobs_with_args(self):
        args = self.parser.parse_args(['dags', 'list_jobs', '--dag_id',
                                       'example_bash_operator',
                                       '--state', 'success',
                                       '--limit', '100',
                                       '--output', 'tsv'])
        cli.list_jobs(args)

    def test_pause(self):
        args = self.parser.parse_args([
            'dags', 'pause', 'example_bash_operator'])
        cli.pause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [True, 1])

        args = self.parser.parse_args([
            'dags', 'unpause', 'example_bash_operator'])
        cli.unpause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].is_paused, [False, 0])

    def test_trigger_dag(self):
        cli.trigger_dag(self.parser.parse_args([
            'dags', 'trigger', 'example_bash_operator',
            '-c', '{"foo": "bar"}']))
        self.assertRaises(
            ValueError,
            cli.trigger_dag,
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
        cli.delete_dag(self.parser.parse_args([
            'dags', 'delete', key, '--yes']))
        self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)
        self.assertRaises(
            AirflowException,
            cli.delete_dag,
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
            cli.delete_dag(self.parser.parse_args([
                'dags', 'delete', key, '--yes']))
            self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)

    def test_cli_list_jobs(self):
        args = self.parser.parse_args(['dags', 'list_jobs'])
        cli.list_jobs(args)

    def test_dag_state(self):
        self.assertEqual(None, cli.dag_state(self.parser.parse_args([
            'dags', 'state', 'example_bash_operator', DEFAULT_DATE.isoformat()])))
