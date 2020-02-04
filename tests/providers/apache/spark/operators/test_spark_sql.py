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
#

import datetime
import unittest

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator

DEFAULT_DATE = datetime.datetime(2017, 1, 1)


class TestSparkSqlOperator(unittest.TestCase):
    _config = {
        'sql': 'SELECT 22',
        'conn_id': 'spark_special_conn_id',
        'total_executor_cores': 4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'master': 'yarn-client',
        'name': 'special-application-name',
        'num_executors': 8,
        'yarn_queue': 'special-queue'
    }

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self):
        # Given / When
        operator = SparkSqlOperator(
            task_id='spark_sql_job',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(self._config['sql'], operator._sql)
        self.assertEqual(self._config['conn_id'], operator._conn_id)
        self.assertEqual(self._config['total_executor_cores'], operator._total_executor_cores)
        self.assertEqual(self._config['executor_cores'], operator._executor_cores)
        self.assertEqual(self._config['executor_memory'], operator._executor_memory)
        self.assertEqual(self._config['keytab'], operator._keytab)
        self.assertEqual(self._config['principal'], operator._principal)
        self.assertEqual(self._config['executor_memory'], operator._executor_memory)
        self.assertEqual(self._config['keytab'], operator._keytab)
        self.assertEqual(self._config['principal'], operator._principal)
        self.assertEqual(self._config['master'], operator._master)
        self.assertEqual(self._config['name'], operator._name)
        self.assertEqual(self._config['num_executors'], operator._num_executors)
        self.assertEqual(self._config['yarn_queue'], operator._yarn_queue)


if __name__ == '__main__':
    unittest.main()
