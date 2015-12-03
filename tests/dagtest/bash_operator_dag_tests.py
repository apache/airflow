"""
    End to end tests of some simple DAGs composed of basic bash_operators
"""

import unittest
from datetime import datetime

from airflow import jobs
from .dag_tester import DagBackfillTest, validate_file_content, validate_order


class BashOperatorSingleOneDay(unittest.TestCase, DagBackfillTest):
    """
    Tests that a bash operator executed over 1 day correctly produces 1 file.
    """
    def get_dag_id(self):
        return "bash_operator_single"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 1))

    def post_check(self, working_dir):
        validate_file_content(working_dir, "out.2015-01-01.txt", "success\n")


class BashOperatorSingle3Days(unittest.TestCase, DagBackfillTest):
    """
    Tests that a bash operator executed over 3 days correctly produces 3 files.
    """
    dates = ["2015-01-01", "2015-01-02", "2015-01-03"]

    def get_dag_id(self):
        return "bash_operator_single"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 3))

    def post_check(self, working_dir):
        for date in self.dates:
            out_file = "out.{date}.txt".format(**locals())
            validate_file_content(working_dir, out_file, "success\n")


class BashOperatorABDownStream(unittest.TestCase, DagBackfillTest):
    """
    Tests that two bash operators linked with .set_downstream that are executed
    over 10 days each produce 10 files in a legal order.

    * A and B
    * B depends on A
    """
    dates = ["2015-01-01", "2015-01-02", "2015-01-03", "2015-01-04",
             "2015-01-05", "2015-01-06", "2015-01-07", "2015-01-08",
             "2015-01-09", "2015-01-10"]

    file_a = "out.a.{date}.txt"
    file_b = "out.b.{date}.txt"

    def get_dag_id(self):
        return "bash_operator_ab"

    def get_test_context(self):
        return {"dep_direction": "downstream"}

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 10))

    def post_check(self, working_dir):
        for date in self.dates:

            file_a_date = self.file_a.format(**locals())
            file_b_date = self.file_b.format(**locals())

            validate_file_content(working_dir, file_a_date, "success_a\n")
            validate_file_content(working_dir, file_b_date, "success_b\n")

            validate_order(working_dir, file_a_date, file_b_date)


class BashOperatorABUpstream(BashOperatorABDownStream, DagBackfillTest):
    """
    Tests that two bash operators linked with .set_upstream that are executed
    over 10 days each produce 10 files in a legal order.

    * A and B
    * B depends on A
    """
    def get_dag_id(self):
        return "bash_operator_ab"

    def get_test_context(self):
        return {"dep_direction": "upstream"}


class BashOperatorABRetries(BashOperatorABDownStream, DagBackfillTest):
    """
    Tests that two bash operators linked with .set_downstream that are executed
    over 10 days each produce 10 files in a legal order. Retries introduce
    chaos.

    * A and B
    * B depends on A
    * A has retries
    """
    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": False,
                "wait_for_downstream": False}


class BashOperatorABDependsOnPast(BashOperatorABDownStream, DagBackfillTest):
    """
    Tests that two bash operators linked with .set_downstream and
    depends_on_past that are executed over 10 days each produce 10 files in a
    legal order. Retries introduce chaos.

    * A and B
    * B depends on A
    * A has retries
    * B depends on past
    """
    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": True,
                "wait_for_downstream": False}

    def post_check(self, working_dir):

        prev_file_b_date = None

        for date in self.dates:
            file_a_date = self.file_a.format(**locals())
            file_b_date = self.file_b.format(**locals())

            validate_file_content(working_dir, file_a_date, "success_a\n")
            validate_file_content(working_dir, file_b_date, "success_b\n")

            validate_order(working_dir, file_a_date, file_b_date)

            if prev_file_b_date:
                validate_order(working_dir, prev_file_b_date, file_b_date)

            prev_file_b_date = file_b_date


class BashOperatorABWaitForDownstream(BashOperatorABDownStream, DagBackfillTest):
    """
    Tests that two bash operators linked with .set_downstream and
    wait_for_downstream that are executed over 10 days each produce 10 files
    in a legal order. Retries introduce chaos.

    * A and B
    * B depends on A
    * A has retries
    * A waits for downstream
    """
    def get_dag_id(self):
        return "bash_operator_ab_retries"

    def get_test_context(self):
        return {"depends_on_past": False,
                "wait_for_downstream": True}

    def post_check(self, working_dir):

        prev_file_b_date = None

        for date in self.dates:
            file_a_date = self.file_a.format(**locals())
            file_b_date = self.file_b.format(**locals())

            validate_file_content(working_dir, file_a_date, "success_a\n")
            validate_file_content(working_dir, file_b_date, "success_b\n")

            validate_order(working_dir, file_a_date, file_b_date)

            if prev_file_b_date:
                validate_order(working_dir, prev_file_b_date, file_a_date)

            prev_file_b_date = file_b_date
