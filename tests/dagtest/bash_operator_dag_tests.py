"""
    End to end tests of simple DAG composed of basic bash_operators
"""

import os
import unittest
from datetime import datetime

from airflow import jobs
from dag_tester import  DagBackfillTest


class SingleBashOperatorDagTest_oneDay(unittest.TestCase, DagBackfillTest):

    def get_dag_id(self):
        return "bashop"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 1))

    def post_check(self):
        with open("/tmp/out.txt") as f:
            assert "success\n" == f.readline()

    def tearDown(self):
        print "removing /tmp/out.txt"
        os.system ("rm -rf /tmp/out.txt")


class SingleBashOperatorDagTest_3Days(unittest.TestCase, DagBackfillTest):

    def get_dag_id(self):
        return "bashop"

    def build_job(self, dag):
        return jobs.BackfillJob(
                dag=dag,
                start_date=datetime(2015, 1, 1),
                end_date=datetime(2015, 1, 3))

    def post_check(self):
        # TODO: we'd like to have a separate file and/or written content per execution date
        with open("/tmp/out.txt") as f:
            assert "success\n" == f.readline()

    def tearDown(self):
        # TODO: we'd like to have a separate file and/or written content per execution date
        print ("removing /tmp/out.txt")
        os.system ("rm -rf /tmp/out.txt")

