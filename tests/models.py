import unittest
import datetime

from airflow import models


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

        assert dagbag.size() == 7

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(include_examples=True)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None
