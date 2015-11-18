import unittest
import datetime

from airflow import models

class DagRunTest(unittest.TestCase):
    def test_id_for_date(self):
        run_id = models.DagRun.id_for_date(datetime.datetime(2015, 01, 02, 03, 04, 05, 06, None))
        assert run_id == 'scheduled__2015-01-02T03:04:05', 'Generated run_id did not match expections: %s' % run_id

