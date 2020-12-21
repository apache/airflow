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
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands import dag_command
from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs

dag_folder_path = '/'.join(os.path.realpath(__file__).split('/')[:-1])

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1))
TEST_DAG_FOLDER = os.path.join(os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'


EXAMPLE_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), "airflow/example_dags"
)


# TODO: Check if tests needs side effects - locally there's missing DAG
class TestCliDags(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.dagbag.sync_to_db()
        cls.parser = cli_parser.get_parser()

    @classmethod
    def tearDownClass(cls) -> None:
        clear_db_runs()
        clear_db_dags()

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
    def test_backfill(self, mock_run):
        dag_command.dag_backfill(
            self.parser.parse_args(
                ['dags', 'backfill', 'example_bash_operator', '--start-date', DEFAULT_DATE.isoformat()]
            )
        )

        mock_run.assert_called_once_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            conf=None,
            delay_on_limit_secs=1.0,
            donot_pickle=False,
            ignore_first_depends_on_past=True,
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

        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            dag_command.dag_backfill(
                self.parser.parse_args(
                    [
                        'dags',
                        'backfill',
                        'example_bash_operator',
                        '--task-regex',
                        'runme_0',
                        '--dry-run',
                        '--start-date',
                        DEFAULT_DATE.isoformat(),
                    ]
                ),
                dag=dag,
            )

        output = stdout.getvalue()
        self.assertIn(f"Dry run of DAG example_bash_operator on {DEFAULT_DATE.isoformat()}\n", output)
        self.assertIn("Task runme_0\n", output)

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        dag_command.dag_backfill(
            self.parser.parse_args(
                [
                    'dags',
                    'backfill',
                    'example_bash_operator',
                    '--dry-run',
                    '--start-date',
                    DEFAULT_DATE.isoformat(),
                ]
            ),
            dag=dag,
        )

        mock_run.assert_not_called()  # Dry run shouldn't run the backfill

        dag_command.dag_backfill(
            self.parser.parse_args(
                [
                    'dags',
                    'backfill',
                    'example_bash_operator',
                    '--local',
                    '--start-date',
                    DEFAULT_DATE.isoformat(),
                ]
            ),
            dag=dag,
        )

        mock_run.assert_called_once_with(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
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
        mock_run.reset_mock()

    def test_show_dag_print(self):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_show(self.parser.parse_args(['dags', 'show', 'example_bash_operator']))
        out = temp_stdout.getvalue()
        self.assertIn("label=example_bash_operator", out)
        self.assertIn("graph [label=example_bash_operator labelloc=t rankdir=LR]", out)
        self.assertIn("runme_2 -> run_after_loop", out)

    @mock.patch("airflow.cli.commands.dag_command.render_dag")
    def test_show_dag_dave(self, mock_render_dag):
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(['dags', 'show', 'example_bash_operator', '--save', 'awesome.png'])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.render.assert_called_once_with(
            cleanup=True, filename='awesome', format='png'
        )
        self.assertIn("File awesome.png saved", out)

    @mock.patch("airflow.cli.commands.dag_command.subprocess.Popen")
    @mock.patch("airflow.cli.commands.dag_command.render_dag")
    def test_show_dag_imgcat(self, mock_render_dag, mock_popen):
        mock_render_dag.return_value.pipe.return_value = b"DOT_DATA"
        mock_popen.return_value.communicate.return_value = (b"OUT", b"ERR")
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_show(
                self.parser.parse_args(['dags', 'show', 'example_bash_operator', '--imgcat'])
            )
        out = temp_stdout.getvalue()
        mock_render_dag.return_value.pipe.assert_called_once_with(format='png')
        mock_popen.return_value.communicate.assert_called_once_with(b'DOT_DATA')
        self.assertIn("OUT", out)
        self.assertIn("ERR", out)

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
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
            '--local',
            '--start-date',
            run_date.isoformat(),
            '--ignore-first-depends-on-past',
        ]
        dag = self.dagbag.get_dag(dag_id)

        dag_command.dag_backfill(self.parser.parse_args(args), dag=dag)

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

    @mock.patch("airflow.cli.commands.dag_command.DAG.run")
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
            '--local',
            '--start-date',
            start_date.isoformat(),
            '--end-date',
            end_date.isoformat(),
            '--ignore-first-depends-on-past',
            '--run-backwards',
        ]
        dag = self.dagbag.get_dag(dag_id)

        dag_command.dag_backfill(self.parser.parse_args(args), dag=dag)
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
        dag_ids = [
            'example_bash_operator',  # schedule_interval is '0 0 * * *'
            'latest_only',  # schedule_interval is timedelta(hours=4)
            'example_python_operator',  # schedule_interval=None
            'example_xcom',
        ]  # schedule_interval="@once"

        # Delete DagRuns
        with create_session() as session:
            dr = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
            dr.delete(synchronize_session=False)

        # Test None output
        args = self.parser.parse_args(['dags', 'next-execution', dag_ids[0]])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_next_execution(args)
            out = temp_stdout.getvalue()
            # `next_execution` function is inapplicable if no execution record found
            # It prints `None` in such cases
            self.assertIn("None", out)

        # The details below is determined by the schedule_interval of example DAGs
        now = DEFAULT_DATE
        expected_output = [str(now + timedelta(days=1)), str(now + timedelta(hours=4)), "None", "None"]
        expected_output_2 = [
            str(now + timedelta(days=1)) + os.linesep + str(now + timedelta(days=2)),
            str(now + timedelta(hours=4)) + os.linesep + str(now + timedelta(hours=8)),
            "None",
            "None",
        ]

        for i, dag_id in enumerate(dag_ids):
            dag = self.dagbag.dags[dag_id]
            # Create a DagRun for each DAG, to prepare for next step
            dag.create_dagrun(
                run_type=DagRunType.MANUAL, execution_date=now, start_date=now, state=State.FAILED
            )

            # Test num-executions = 1 (default)
            args = self.parser.parse_args(['dags', 'next-execution', dag_id])
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            self.assertIn(expected_output[i], out)

            # Test num-executions = 2
            args = self.parser.parse_args(['dags', 'next-execution', dag_id, '--num-executions', '2'])
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                dag_command.dag_next_execution(args)
                out = temp_stdout.getvalue()
            self.assertIn(expected_output_2[i], out)

        # Clean up before leaving
        with create_session() as session:
            dr = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
            dr.delete(synchronize_session=False)

    @conf_vars({('core', 'load_examples'): 'true'})
    def test_cli_report(self):
        args = self.parser.parse_args(['dags', 'report', '--output', 'json'])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_report(args)
            out = temp_stdout.getvalue()

        self.assertIn("airflow/example_dags/example_complex.py", out)
        self.assertIn("example_complex", out)

    @conf_vars({('core', 'load_examples'): 'true'})
    def test_cli_list_dags(self):
        args = self.parser.parse_args(['dags', 'list', '--output', 'yaml'])
        with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
            dag_command.dag_list_dags(args)
            out = temp_stdout.getvalue()
        self.assertIn("owner", out)
        self.assertIn("airflow", out)
        self.assertIn("paused", out)
        self.assertIn("airflow/example_dags/example_complex.py", out)
        self.assertIn("False", out)

    def test_cli_list_dag_runs(self):
        dag_command.dag_trigger(
            self.parser.parse_args(
                [
                    'dags',
                    'trigger',
                    'example_bash_operator',
                ]
            )
        )
        args = self.parser.parse_args(
            [
                'dags',
                'list-runs',
                '--dag-id',
                'example_bash_operator',
                '--no-backfill',
                '--start-date',
                DEFAULT_DATE.isoformat(),
                '--end-date',
                timezone.make_aware(datetime.max).isoformat(),
            ]
        )
        dag_command.dag_list_dag_runs(args)

    def test_cli_list_jobs_with_args(self):
        args = self.parser.parse_args(
            [
                'dags',
                'list-jobs',
                '--dag-id',
                'example_bash_operator',
                '--state',
                'success',
                '--limit',
                '100',
                '--output',
                'json',
            ]
        )
        dag_command.dag_list_jobs(args)

    def test_pause(self):
        args = self.parser.parse_args(['dags', 'pause', 'example_bash_operator'])
        dag_command.dag_pause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].get_is_paused(), [True, 1])

        args = self.parser.parse_args(['dags', 'unpause', 'example_bash_operator'])
        dag_command.dag_unpause(args)
        self.assertIn(self.dagbag.dags['example_bash_operator'].get_is_paused(), [False, 0])

    def test_trigger_dag(self):
        dag_command.dag_trigger(
            self.parser.parse_args(['dags', 'trigger', 'example_bash_operator', '--conf', '{"foo": "bar"}'])
        )
        self.assertRaises(
            ValueError,
            dag_command.dag_trigger,
            self.parser.parse_args(
                [
                    'dags',
                    'trigger',
                    'example_bash_operator',
                    '--run-id',
                    'trigger_dag_xxx',
                    '--conf',
                    'NOT JSON',
                ]
            ),
        )

    def test_delete_dag(self):
        DM = DagModel
        key = "my_dag_id"
        session = settings.Session()
        session.add(DM(dag_id=key))
        session.commit()
        dag_command.dag_delete(self.parser.parse_args(['dags', 'delete', key, '--yes']))
        self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)
        self.assertRaises(
            AirflowException,
            dag_command.dag_delete,
            self.parser.parse_args(['dags', 'delete', 'does_not_exist_dag', '--yes']),
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
            dag_command.dag_delete(self.parser.parse_args(['dags', 'delete', key, '--yes']))
            self.assertEqual(session.query(DM).filter_by(dag_id=key).count(), 0)

    def test_cli_list_jobs(self):
        args = self.parser.parse_args(['dags', 'list-jobs'])
        dag_command.dag_list_jobs(args)

    def test_dag_state(self):
        self.assertEqual(
            None,
            dag_command.dag_state(
                self.parser.parse_args(['dags', 'state', 'example_bash_operator', DEFAULT_DATE.isoformat()])
            ),
        )

    @mock.patch("airflow.cli.commands.dag_command.DebugExecutor")
    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    def test_dag_test(self, mock_get_dag, mock_executor):
        cli_args = self.parser.parse_args(['dags', 'test', 'example_bash_operator', DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id='example_bash_operator'),
                mock.call().clear(
                    start_date=cli_args.execution_date,
                    end_date=cli_args.execution_date,
                    dag_run_state=State.NONE,
                ),
                mock.call().run(
                    executor=mock_executor.return_value,
                    start_date=cli_args.execution_date,
                    end_date=cli_args.execution_date,
                ),
            ]
        )

    @mock.patch(
        "airflow.cli.commands.dag_command.render_dag", **{'return_value.source': "SOURCE"}  # type: ignore
    )
    @mock.patch("airflow.cli.commands.dag_command.DebugExecutor")
    @mock.patch("airflow.cli.commands.dag_command.get_dag")
    def test_dag_test_show_dag(self, mock_get_dag, mock_executor, mock_render_dag):
        cli_args = self.parser.parse_args(
            ['dags', 'test', 'example_bash_operator', DEFAULT_DATE.isoformat(), '--show-dagrun']
        )
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            dag_command.dag_test(cli_args)

        output = stdout.getvalue()

        mock_get_dag.assert_has_calls(
            [
                mock.call(subdir=cli_args.subdir, dag_id='example_bash_operator'),
                mock.call().clear(
                    start_date=cli_args.execution_date,
                    end_date=cli_args.execution_date,
                    dag_run_state=State.NONE,
                ),
                mock.call().run(
                    executor=mock_executor.return_value,
                    start_date=cli_args.execution_date,
                    end_date=cli_args.execution_date,
                ),
            ]
        )
        mock_render_dag.assert_has_calls([mock.call(mock_get_dag.return_value, tis=[])])
        self.assertIn("SOURCE", output)
