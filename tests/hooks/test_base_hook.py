import unittest
import os

from airflow.hooks.base_hook import BaseHook

class TestBaseHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.base_hook = BaseHook()

    def tearDown(self):
        del os.environ['AIRFLOW_CTX_DAG_ID']

    def test_connection_permitted(self):

        os.environ['AIRFLOW_CTX_DAG_ID'] = 'dag_1'
        assert self.base_hook.is_connection_permitted('conn_2',
                                                      [{
                                                          'connection': 'conn_1',
                                                          'dags': ['dag_1', 'dag_2']
                                                       },{
                                                          'connection': 'conn_2',
                                                          'dags': ['dag_3']
                                                       }
                                                      ]) is False

        os.environ['AIRFLOW_CTX_DAG_ID'] = 'dag_2'
        assert self.base_hook.is_connection_permitted('conn_1',
                                                      [{
                                                          'connection': 'conn_1',
                                                          'dags': ['dag_1', 'dag_2']
                                                       },{
                                                          'connection': 'conn_2',
                                                          'dags': ['dag_3']
                                                       }
                                                      ]) is True

        os.environ['AIRFLOW_CTX_DAG_ID'] = 'dag_3'
        assert self.base_hook.is_connection_permitted('conn_1',
                                                      [{
                                                          'connection': 'conn_1',
                                                          'dags': ['dag_1', 'dag_2']
                                                       },{
                                                          'connection': 'conn_2',
                                                          'dags': ['dag_3']
                                                       }
                                                      ]) is False
