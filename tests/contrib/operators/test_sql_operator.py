# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from airflow.contrib.operators.sql_operator import SqlOperator
from airflow import DAG
from airflow import configuration
from airflow.utils.tests import skipUnlessImported
from airflow.utils import timezone
import unittest


class SqlOperatorTest(unittest.TestCase):

    DEFAULT_DATE = timezone.datetime(2015, 1, 1)

    def setUp(self):
        configuration.load_test_config()
        args = {'owner': 'airflow', 'start_date': self.DEFAULT_DATE}
        dag = DAG('unit_test_dag', default_args=args)
        self.dag = dag

    @skipUnlessImported('airflow.operators.postgres_operator', 'PostgresOperator')
    def test_sql_operator_postgres_test(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        task_basic = SqlOperator(task_id='sql_operator_postgres_test',
                                 sql=sql,
                                 conn_id='postgres_default',
                                 dag=self.dag)
        task_basic.run(start_date=self.DEFAULT_DATE,
                       end_date=self.DEFAULT_DATE,
                       ignore_ti_state=True)

        task_autocommit = SqlOperator(task_id='sql_operator_postgres_test_autocommit',
                                      sql=sql,
                                      conn_id='postgres_default',
                                      dag=self.dag,
                                      autocommit=True)
        task_autocommit.run(start_date=self.DEFAULT_DATE,
                            end_date=self.DEFAULT_DATE,
                            ignore_ti_state=True)
        sql = [
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        task_multi = SqlOperator(task_id='sql_operator_postgres_test_multi',
                                 sql=sql,
                                 conn_id='postgres_default',
                                 dag=self.dag)
        task_multi.run(start_date=self.DEFAULT_DATE,
                       end_date=self.DEFAULT_DATE,
                       ignore_ti_state=True)

    @skipUnlessImported('airflow.operators.mysql_operator', 'MySqlOperator')
    def test_sql_operator_mysql_test(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        task_basic = SqlOperator(task_id='sql_operator_mysql_test',
                                 sql=sql,
                                 conn_id='airflow_db',
                                 dag=self.dag)
        task_basic.run(start_date=self.DEFAULT_DATE,
                       end_date=self.DEFAULT_DATE,
                       ignore_ti_state=True)

        task_autocommit = SqlOperator(task_id='sql_operator_mysql_test_autocommit',
                                      sql=sql,
                                      conn_id='airflow_db',
                                      dag=self.dag,
                                      autocommit=True)
        task_autocommit.run(start_date=self.DEFAULT_DATE,
                            end_date=self.DEFAULT_DATE,
                            ignore_ti_state=True)
        sql = [
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        task_multi = SqlOperator(task_id='sql_operator_mysql_test_multi',
                                 sql=sql,
                                 conn_id='airflow_db',
                                 dag=self.dag)
        task_multi.run(start_date=self.DEFAULT_DATE,
                       end_date=self.DEFAULT_DATE,
                       ignore_ti_state=True)
