# -*- coding:utf-8 -*-

import datetime
from dags import curve_store_dag
from unittest import TestCase
from airflow.models import DagBag
from airflow.models.taskinstance import TaskInstance
from dags.curve_store_dag import storeTaskArgs
from airflow.entities.result_storage import ClsResultStorage


class TestQueryInfluxdata(TestCase):
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

    def test_QueryInfluxData(self):
        args = storeTaskArgs.get('resultArgs')
        args.update(
            {'token': 'ZOtYmpMWvOzsSt6CK95-NsUMAj1ZsrBKABTOgIkK88Otn0GIA_vK7omEiXSq0OvEFmHj08CK0pivDFcW-isoLg=='})
        # args.update({
        #     "url": "127.0.0.1:9999"
        # })
        query_str = '''from(bucket: "desoutter")
            |> range(start: 0, stop: now())
            |> filter(fn: (r) => r._measurement == "results")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
        st = ClsResultStorage()
        st.ensure_connect()
        tables = st.query_api.query(query_str)
        for table in tables:
            print(table)
            for row in table.records:
                print(row.values)

    def test_QueryResult(self):
        args = storeTaskArgs.get('resultArgs')
        args.update(
            {'token': 'ZOtYmpMWvOzsSt6CK95-NsUMAj1ZsrBKABTOgIkK88Otn0GIA_vK7omEiXSq0OvEFmHj08CK0pivDFcW-isoLg=='})
        # args.update({
        #     "url": "127.0.0.1:9999"
        # })
        st = ClsResultStorage()
        st.metadata = {'entity_id': '3002/0000002056/1585765585'}
        result = st.query_result()
        print(result)
