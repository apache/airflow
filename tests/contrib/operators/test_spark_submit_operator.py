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
import sys

from airflow import DAG, configuration
from airflow.models import TaskInstance

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils import timezone

from datetime import timedelta

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSparkSubmitOperator(unittest.TestCase):

    _config = {
        'conf': {
            'parquet.compression': 'SNAPPY'
        },
        'files': 'hive-site.xml',
        'py_files': 'sample_library.py',
        'driver_classpath': 'parquet.jar',
        'jars': 'parquet.jar',
        'packages': 'com.databricks:spark-avro_2.11:3.2.0',
        'exclude_packages': 'org.bad.dependency:1.0.0',
        'repositories': 'http://myrepo.org',
        'total_executor_cores':4,
        'executor_cores': 4,
        'executor_memory': '22g',
        'keytab': 'privileged_user.keytab',
        'principal': 'user/spark@airflow.org',
        'name': '{{ task_instance.task_id }}',
        'num_executors': 10,
        'verbose': True,
        'application': 'test_application.py',
        'driver_memory': '3g',
        'java_class': 'com.foo.bar.AppMain',
        'application_args': [
            '-f', 'foo',
            '--bar', 'bar',
            '--start', '{{ macros.ds_add(ds, -1)}}',
            '--end', '{{ ds }}',
            '--with-spaces', 'args should keep embdedded spaces',
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

    def test_execute(self):
        # Given / When
        conn_id = 'spark_default'
        operator = SparkSubmitOperator(
            task_id='spark_submit_job',
            dag=self.dag,
            **self._config
        )

        # Then
        expected_dict = {
            'conf': {
                'parquet.compression': 'SNAPPY'
            },
            'files': 'hive-site.xml',
            'py_files': 'sample_library.py',
            'driver_classpath': 'parquet.jar',
            'jars': 'parquet.jar',
            'packages': 'com.databricks:spark-avro_2.11:3.2.0',
            'exclude_packages': 'org.bad.dependency:1.0.0',
            'repositories': 'http://myrepo.org',
            'total_executor_cores': 4,
            'executor_cores': 4,
            'executor_memory': '22g',
            'keytab': 'privileged_user.keytab',
            'principal': 'user/spark@airflow.org',
            'name': '{{ task_instance.task_id }}',
            'num_executors': 10,
            'verbose': True,
            'application': 'test_application.py',
            'driver_memory': '3g',
            'java_class': 'com.foo.bar.AppMain',
            'application_args': [
                '-f', 'foo',
                '--bar', 'bar',
                '--start', '{{ macros.ds_add(ds, -1)}}',
                '--end', '{{ ds }}',
                '--with-spaces', 'args should keep embdedded spaces',
            ]

        }

        self.assertEqual(conn_id, operator._conn_id)
        self.assertEqual(expected_dict['application'], operator._application)
        self.assertEqual(expected_dict['conf'], operator._conf)
        self.assertEqual(expected_dict['files'], operator._files)
        self.assertEqual(expected_dict['py_files'], operator._py_files)
        self.assertEqual(expected_dict['driver_classpath'], operator._driver_classpath)
        self.assertEqual(expected_dict['jars'], operator._jars)
        self.assertEqual(expected_dict['packages'], operator._packages)
        self.assertEqual(expected_dict['exclude_packages'], operator._exclude_packages)
        self.assertEqual(expected_dict['repositories'], operator._repositories)
        self.assertEqual(expected_dict['total_executor_cores'], operator._total_executor_cores)
        self.assertEqual(expected_dict['executor_cores'], operator._executor_cores)
        self.assertEqual(expected_dict['executor_memory'], operator._executor_memory)
        self.assertEqual(expected_dict['keytab'], operator._keytab)
        self.assertEqual(expected_dict['principal'], operator._principal)
        self.assertEqual(expected_dict['name'], operator._name)
        self.assertEqual(expected_dict['num_executors'], operator._num_executors)
        self.assertEqual(expected_dict['verbose'], operator._verbose)
        self.assertEqual(expected_dict['java_class'], operator._java_class)
        self.assertEqual(expected_dict['driver_memory'], operator._driver_memory)
        self.assertEqual(expected_dict['application_args'], operator._application_args)

    def test_render_template(self):
        # Given
        operator = SparkSubmitOperator(task_id='spark_submit_job', dag=self.dag, **self._config)
        ti = TaskInstance(operator, DEFAULT_DATE)

        # When
        ti.render_templates()

        # Then
        expected_application_args = [u'-f', 'foo',
                                     u'--bar', 'bar',
                                     u'--start', (DEFAULT_DATE - timedelta(days=1)).strftime("%Y-%m-%d"),
                                     u'--end', DEFAULT_DATE.strftime("%Y-%m-%d"),
                                     u'--with-spaces', u'args should keep embdedded spaces',
                                     ]
        expected_name = "spark_submit_job"
        self.assertListEqual(expected_application_args, getattr(operator, '_application_args'))
        self.assertEqual(expected_name, getattr(operator, '_name'))


if __name__ == '__main__':
    unittest.main()
