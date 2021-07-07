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

from dags import curve_store_dag


class TestDoStoreTask(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def setUp(self):
        self.dag = self.dagbag.get_dag(dag_id=curve_store_dag.DAG_ID)
        self.storeTask = self.dag.get_task(curve_store_dag.STORE_TASK)
        self.storeTaskTi = TaskInstance(task=self.dag.task_dict.get(curve_store_dag.STORE_TASK),
                                        execution_date=datetime.datetime.now())

    def test_loadSCurveAnayDag(self):
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 1)

    def test_doStoreTask(self):
        context = self.storeTaskTi.get_template_context()
        kargs = self.storeTask.op_kwargs
        kargs.update({'test_mode': True})
        kargs.update({'params': tt.body})  # 模拟与restful API一样的数据源
        self.storeTask.execute(context)
