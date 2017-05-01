# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import datetime
import sys

from airflow import DAG, configuration
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

DEFAULT_DATE = datetime.datetime(2017, 1, 1)


class TestSparkSubmitOperator(unittest.TestCase):

    _config = {
        'conf': {
            'parquet.compression': 'SNAPPY'
        },
        'files': 'hive-site.xml',
        'py_files': 'sample_library.py',
        'jars': 'parquet.jar',
        'total_executor_cores':4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'name': 'spark-job',
        'num_executors': 10,
        'verbose': True,
        'application': 'test_application.py',
        'driver_memory': '3g',
        'java_class': 'com.foo.bar.AppMain',
        'application_args': [
            '-f foo',
            '--bar bar'
        ]
    }

    def setUp(self):

        if sys.version_info[0] == 3:
            raise unittest.SkipTest('TestSparkSubmitOperator won\'t work with '
                                    'python3. No need to test anything here')

        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self, conn_id='spark_default'):
        operator = SparkSubmitOperator(
            task_id='spark_submit_job',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(conn_id, operator._conn_id)

        self.assertEqual(self._config['application'], operator._application)
        self.assertEqual(self._config['conf'], operator._conf)
        self.assertEqual(self._config['files'], operator._files)
        self.assertEqual(self._config['py_files'], operator._py_files)
        self.assertEqual(self._config['jars'], operator._jars)
        self.assertEqual(self._config['total_executor_cores'], operator._total_executor_cores)
        self.assertEqual(self._config['executor_cores'], operator._executor_cores)
        self.assertEqual(self._config['executor_memory'], operator._executor_memory)
        self.assertEqual(self._config['keytab'], operator._keytab)
        self.assertEqual(self._config['principal'], operator._principal)
        self.assertEqual(self._config['name'], operator._name)
        self.assertEqual(self._config['num_executors'], operator._num_executors)
        self.assertEqual(self._config['verbose'], operator._verbose)
        self.assertEqual(self._config['java_class'], operator._java_class)
        self.assertEqual(self._config['driver_memory'], operator._driver_memory)
        self.assertEqual(self._config['application_args'], operator._application_args)




if __name__ == '__main__':
    unittest.main()
