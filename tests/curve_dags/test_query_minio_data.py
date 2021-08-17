# -*- coding:utf-8 -*-

import datetime
from dags import curve_store_dag
from unittest import TestCase
from airflow.models import DagBag
from airflow.models.taskinstance import TaskInstance
from dags.curve_store_dag import storeTaskArgs
from plugins.entities.curve_storage import ClsCurveStorage
from airflow.www_rbac.api.experimental.utils import get_curve_args


class TestQueryMinioData(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

    def setUp(self):
        self.dag = self.dagbag.get_dag(dag_id=curve_store_dag.DAG_ID)
        self.task = self.dag.get_task(curve_store_dag.TRIGGER_ANAY_TASK)
        self.taskInstance = TaskInstance(task=self.dag.task_dict.get(curve_store_dag.TRIGGER_ANAY_TASK),
                                         execution_date=datetime.datetime.now())

    def test_loadSCurveAnayDag(self):
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 1)

    def test_QueryMinioData(self):
        curve_args = get_curve_args()
        st = ClsCurveStorage(**curve_args)
        st.metadata = {'entity_id': '3002/0000002062/1585767133'}
        curve = st.query_curve()
        print(curve)
