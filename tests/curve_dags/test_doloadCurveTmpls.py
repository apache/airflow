# -*- coding:utf-8 -*-

from unittest import TestCase
from airflow.models import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
import datetime

try:
    from . import test_trigger as tt
except ImportError:
    from test.test_trigger import tt

from dags import load_all_curve_template


class TestDoStoreTask(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def setUp(self):
        self.dag = self.dagbag.get_dag(dag_id=load_all_curve_template.DAG_ID)
        self.storeTask = self.dag.get_task(load_all_curve_template.STORE_TASK)
        self.storeTaskTi = TaskInstance(task=self.dag.task_dict.get(load_all_curve_template.STORE_TASK),
                                        execution_date=datetime.datetime.now())

    def test_loadSCurveAnayDag(self):
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 1)

    def test_doStoreTask(self):
        context = self.storeTaskTi.get_template_context()
        kargs = self.storeTask.op_kwargs
        kargs.update({'test_mode': True})
        self.storeTask.execute(context)
