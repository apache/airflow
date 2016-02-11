# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
import datetime

from airflow import models
from airflow.operators.dummy_operator import DummyOperator


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
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_run_pooling_task_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = models.TaskInstance(task=task, execution_date=datetime.datetime.now())
        ti.run()
        assert ti.state == models.State.QUEUED

    def test_run_pooling_task_with_mark_success(self):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(task_id='test_run_pooling_task_with_mark_success_op', dag=dag,
                             pool='test_run_pooling_task_with_mark_success_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = models.TaskInstance(task=task, execution_date=datetime.datetime.now())
        ti.run(mark_success=True)
        assert ti.state == models.State.SUCCESS
