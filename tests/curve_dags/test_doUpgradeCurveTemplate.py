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

from dags import upgrade_curve_template


class TestDoStoreTask(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def setUp(self):
        self.dag = self.dagbag.get_dag(dag_id=upgrade_curve_template.DAG_ID)
        self.upgradeCurveTmplTask = self.dag.get_task(upgrade_curve_template.CURVE_TEMPLATE_UPGRADE_TASK)
        self.upgradeCurveTmplTaskTi = TaskInstance(task=self.dag.task_dict.get(upgrade_curve_template.CURVE_TEMPLATE_UPGRADE_TASK),
                                                   execution_date=datetime.datetime.now())

    def test_loadSCurveAnayDag(self):
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 1)

    def test_doUpgradeCurveTmplTask(self):
        context = self.upgradeCurveTmplTaskTi.get_template_context()
        kargs = self.upgradeCurveTmplTask.op_kwargs
        kargs.update({'test_mode': True})
        self.upgradeCurveTmplTask.execute(context)
