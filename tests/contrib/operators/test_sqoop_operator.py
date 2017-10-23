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

import datetime
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.sqoop_operator import SqoopOperator
from airflow.exceptions import AirflowException


class TestSqoopOperator(unittest.TestCase):
    _config = {
        'cmd_type': 'export',
        'table': 'target_table',
        'query': 'SELECT * FROM schema.table',
        'target_dir': '/path/on/hdfs/to/import',
        'append': True,
        'file_type': 'avro',
        'columns': 'a,b,c',
        'num_mappers': 22,
        'split_by': 'id',
        'export_dir': '/path/on/hdfs/to/export',
        'input_null_string': '\n',
        'input_null_non_string': '\t',
        'staging_table': 'target_table_staging',
        'clear_staging_table': True,
        'enclosed_by': '"',
        'escaped_by': '\\',
        'input_fields_terminated_by': '|',
        'input_lines_terminated_by': '\n',
        'input_optionally_enclosed_by': '"',
        'batch': True,
        'relaxed_isolation': True,
        'direct': True,
        'driver': 'com.microsoft.jdbc.sqlserver.SQLServerDriver',
        'hcatalog_database': 'hive_database',
        'hcatalog_table': 'hive_table',
        'properties': {
            'mapred.map.max.attempts': '1'
        }
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self, conn_id='sqoop_default'):
        operator = SqoopOperator(
            task_id='sqoop_job',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(conn_id, operator.conn_id)

        self.assertEqual(self._config['cmd_type'], operator.cmd_type)
        self.assertEqual(self._config['table'], operator.table)
        self.assertEqual(self._config['target_dir'], operator.target_dir)
        self.assertEqual(self._config['append'], operator.append)
        self.assertEqual(self._config['file_type'], operator.file_type)
        self.assertEqual(self._config['num_mappers'], operator.num_mappers)
        self.assertEqual(self._config['split_by'], operator.split_by)
        self.assertEqual(self._config['input_null_string'],
                         operator.input_null_string)
        self.assertEqual(self._config['input_null_non_string'],
                         operator.input_null_non_string)
        self.assertEqual(self._config['staging_table'], operator.staging_table)
        self.assertEqual(self._config['clear_staging_table'],
                         operator.clear_staging_table)
        self.assertEqual(self._config['batch'], operator.batch)
        self.assertEqual(self._config['relaxed_isolation'],
                         operator.relaxed_isolation)
        self.assertEqual(self._config['direct'], operator.direct)
        self.assertEqual(self._config['driver'], operator.driver)
        self.assertEqual(self._config['properties'], operator.properties)
        self.assertEqual(self._config['hcatalog_database'], operator.hcatalog_database)
        self.assertEqual(self._config['hcatalog_table'], operator.hcatalog_table)

    def test_invalid_cmd_type(self):
        operator = SqoopOperator(task_id='sqoop_job', dag=self.dag,
                                 cmd_type='invalid')
        with self.assertRaises(AirflowException):
            operator.execute({})


if __name__ == '__main__':
    unittest.main()
