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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import os
import unittest
import time

from airflow import models, AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance as TI
from airflow.models import State as ST
from airflow.operators import DummyOperator, BashOperator, PythonOperator
from airflow.utils.state import State
from nose_parameterized import parameterized

DEFAULT_DATE = datetime.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')

class DagTest(unittest.TestCase):

    def test_parms_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = models.DAG('test-dag')

        assert type(dag.params) == dict
        assert len(dag.params) == 0

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {'parameter1': 1}
        params2 = {'parameter2': 2}

        dag = models.DAG('test-dag',
                         default_args={'params': params1},
                         params=params2)

        params_combined = params1.copy()
        params_combined.update(params2)
        assert dag.params == params_combined


class DagRunTest(unittest.TestCase):
    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(
            datetime.datetime(2015, 1, 2, 3, 4, 5, 6, None))
        assert run_id == 'scheduled__2015-01-02T03:04:05', (
            'Generated run_id did not match expectations: {0}'.format(run_id))


class DagBagTest(unittest.TestCase):

    def test_get_existing_dag(self):
        """
        test that were're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(include_examples=True)

        some_expected_dag_ids = ["example_bash_operator",
                                 "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag.dag_id == dag_id

        assert dagbag.size() >= 7

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(include_examples=True)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None

    def test_process_file_that_contains_multi_bytes_char(self):
        """
        test that we're able to parse file that contains multi-byte char
        """
        from tempfile import NamedTemporaryFile
        f = NamedTemporaryFile()
        f.write('\u3042'.encode('utf8'))  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(include_examples=True)
        assert dagbag.process_file(f.name) == []


class TaskInstanceTest(unittest.TestCase):

    def test_run_pooling_task(self):
        """
        test that running task with mark_success param update task state as
        SUCCESS without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_run_pooling_task_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run()
        self.assertEqual(ti.state, models.State.QUEUED)

    def test_run_pooling_task_with_mark_success(self):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(
            task_id='test_run_pooling_task_with_mark_success_op',
            dag=dag,
            pool='test_run_pooling_task_with_mark_success_pool',
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run(mark_success=True)
        self.assertEqual(ti.state, models.State.SUCCESS)

    def test_run_pooling_task_with_skip(self):
        """
        test that running task which returns AirflowSkipOperator will end
        up in a SKIPPED state.
        """

        def raise_skip_exception():
            raise AirflowSkipException

        dag = models.DAG(dag_id='test_run_pooling_task_with_skip')
        task = PythonOperator(
            task_id='test_run_pooling_task_with_skip',
            dag=dag,
            python_callable=raise_skip_exception,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run()
        self.assertTrue(ti.state == models.State.SKIPPED)


    def test_retry_delay(self):
        """
        Test that retry delays are respected
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=3),
            dag=dag,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        # second run -- still up for retry because retry_delay hasn't expired
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

        # third run -- failed
        time.sleep(3)
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)

    def test_retry_handling(self):
        """
        Test that task retries are handled properly
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=0),
            dag=dag,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        # second run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 2)

        # third run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 3)

        # fourth run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 4)

    def test_depends_on_past(self):
        dagbag = models.DagBag()
        dag = dagbag.get_dag('test_depends_on_past')
        dag.clear()
        task = dag.tasks[0]
        run_date = task.start_date + datetime.timedelta(days=5)
        ti = TI(task, run_date)

        # depends_on_past prevents the run
        task.run(start_date=run_date, end_date=run_date)
        ti.refresh_from_db()
        self.assertIs(ti.state, None)

        # ignore first depends_on_past to allow the run
        task.run(
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done
    @parameterized.expand([

        #
        # Tests for all_success
        #
        ['all_success', 5, 0, 0, 0, 0, True, None, True],
        ['all_success', 2, 0, 0, 0, 0, True, None, False],
        ['all_success', 2, 0, 1, 0, 0, True, ST.UPSTREAM_FAILED, False],
        ['all_success', 2, 1, 0, 0, 0, True, ST.SKIPPED, False],
        #
        # Tests for one_success
        #
        ['one_success', 5, 0, 0, 0, 5, True, None, True],
        ['one_success', 2, 0, 0, 0, 2, True, None, True],
        ['one_success', 2, 0, 1, 0, 3, True, None, True],
        ['one_success', 2, 1, 0, 0, 3, True, None, True],
        #
        # Tests for all_failed
        #
        ['all_failed', 5, 0, 0, 0, 5, True, ST.SKIPPED, False],
        ['all_failed', 0, 0, 5, 0, 5, True, None, True],
        ['all_failed', 2, 0, 0, 0, 2, True, ST.SKIPPED, False],
        ['all_failed', 2, 0, 1, 0, 3, True, ST.SKIPPED, False],
        ['all_failed', 2, 1, 0, 0, 3, True, ST.SKIPPED, False],
        #
        # Tests for one_failed
        #
        ['one_failed', 5, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 1, 0, 0, True, None, True],
        ['one_failed', 2, 1, 0, 0, 3, True, None, False],
        ['one_failed', 2, 3, 0, 0, 5, True, ST.SKIPPED, False],
        #
        # Tests for done
        #
        ['all_done', 5, 0, 0, 0, 5, True, None, True],
        ['all_done', 2, 0, 0, 0, 2, True, None, False],
        ['all_done', 2, 0, 1, 0, 3, True, None, False],
        ['all_done', 2, 1, 0, 0, 3, True, None, False]
    ])
    def test_check_task_dependencies(self, trigger_rule, successes, skipped,
                                     failed, upstream_failed, done,
                                     flag_upstream_failed,
                                     expect_state, expect_completed):
        start_date = datetime.datetime(2016, 2, 1, 0, 0, 0)
        dag = models.DAG('test-dag', start_date=start_date)
        downstream = DummyOperator(task_id='downstream',
                                   dag=dag, owner='airflow',
                                   trigger_rule=trigger_rule)
        for i in range(5):
            task = DummyOperator(task_id='runme_{}'.format(i),
                                 dag=dag, owner='airflow')
            task.set_downstream(downstream)
        run_date = task.start_date + datetime.timedelta(days=5)

        ti = TI(downstream, run_date)
        completed = ti.evaluate_trigger_rule(
            successes=successes, skipped=skipped, failed=failed,
            upstream_failed=upstream_failed, done=done,
            flag_upstream_failed=flag_upstream_failed)

        self.assertEqual(completed, expect_completed)
        self.assertEqual(ti.state, expect_state)

