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
#

import datetime
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.sqoop_operator import SqoopOperator
from airflow.exceptions import AirflowException


class TestSqoopOperator(unittest.TestCase):
    _config = {
        'conn_id': 'sqoop_default',
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
        'create_hcatalog_table': True,
        'hcatalog_database': 'hive_database',
        'hcatalog_table': 'hive_table',
        'properties': {
            'mapred.map.max.attempts': '1'
        },
        'extra_import_options': {
            'hcatalog-storage-stanza': "\"stored as orcfile\"",
            'show': ''
        },
        'extra_export_options': {
            'update-key': 'id',
            'update-mode': 'allowinsert'
        }
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_execute(self):
        """
        Tests to verify values of the SqoopOperator match that passed in from the config.
        """
        operator = SqoopOperator(
            task_id='sqoop_job',
            dag=self.dag,
            **self._config
        )

        self.assertEqual(self._config['conn_id'], operator.conn_id)
        self.assertEqual(self._config['query'], operator.query)
        self.assertEqual(self._config['cmd_type'], operator.cmd_type)
        self.assertEqual(self._config['table'], operator.table)
        self.assertEqual(self._config['target_dir'], operator.target_dir)
        self.assertEqual(self._config['append'], operator.append)
        self.assertEqual(self._config['file_type'], operator.file_type)
        self.assertEqual(self._config['num_mappers'], operator.num_mappers)
        self.assertEqual(self._config['split_by'], operator.split_by)
        self.assertEqual(self._config['input_null_string'], operator.input_null_string)
        self.assertEqual(self._config['input_null_non_string'], operator.input_null_non_string)
        self.assertEqual(self._config['staging_table'], operator.staging_table)
        self.assertEqual(self._config['clear_staging_table'], operator.clear_staging_table)
        self.assertEqual(self._config['batch'], operator.batch)
        self.assertEqual(self._config['relaxed_isolation'], operator.relaxed_isolation)
        self.assertEqual(self._config['direct'], operator.direct)
        self.assertEqual(self._config['driver'], operator.driver)
        self.assertEqual(self._config['properties'], operator.properties)
        self.assertEqual(self._config['hcatalog_database'], operator.hcatalog_database)
        self.assertEqual(self._config['hcatalog_table'], operator.hcatalog_table)
        self.assertEqual(self._config['create_hcatalog_table'], operator.create_hcatalog_table)
        self.assertEqual(self._config['extra_import_options'], operator.extra_import_options)
        self.assertEqual(self._config['extra_export_options'], operator.extra_export_options)

        # the following are meant to be more of examples
        SqoopOperator(
            task_id='sqoop_import_using_table',
            cmd_type='import',
            conn_id='sqoop_default',
            table='company',
            verbose=True,
            num_mappers=8,
            hcatalog_database='default',
            hcatalog_table='import_table_1',
            create_hcatalog_table=True,
            extra_import_options={'hcatalog-storage-stanza': "\"stored as orcfile\""},
            dag=self.dag
        )

        SqoopOperator(
            task_id='sqoop_import_using_query',
            cmd_type='import',
            conn_id='sqoop_default',
            query='select name, age from company where $CONDITIONS',
            split_by='age',
            # the mappers will pass in values to the $CONDITIONS based on the field you select to split by
            verbose=True,
            num_mappers=None,
            hcatalog_database='default',
            hcatalog_table='import_table_2',
            create_hcatalog_table=True,
            extra_import_options={'hcatalog-storage-stanza': "\"stored as orcfile\""},
            dag=self.dag
        )

        SqoopOperator(
            task_id='sqoop_import_with_partition',
            cmd_type='import',
            conn_id='sqoop_default',
            table='company',
            verbose=True,
            num_mappers=None,
            hcatalog_database='default',
            hcatalog_table='import_table_3',
            create_hcatalog_table=True,
            extra_import_options={
                'hcatalog-storage-stanza': "\"stored as orcfile\"",
                'hive-partition-key': 'day',
                'hive-partition-value': '2017-10-18'},
            dag=self.dag
        )

        SqoopOperator(
            task_id='sqoop_export_tablename',
            cmd_type='export',
            conn_id='sqoop_default',
            table='rbdms_export_table_1',
            verbose=True,
            num_mappers=None,
            hcatalog_database='default',
            hcatalog_table='hive_export_table_1',
            extra_export_options=None,
            dag=self.dag
        )

        SqoopOperator(
            task_id='sqoop_export_tablepath',
            cmd_type='export',
            conn_id='sqoop_default',
            table='rbdms_export_table_2',
            export_dir='/user/hive/warehouse/export_table_2',
            direct=True,  # speeds up for data transfer
            verbose=True,
            num_mappers=None,
            extra_export_options=None,
            dag=self.dag
        )

    def test_invalid_cmd_type(self):
        """
        Tests to verify if the cmd_type is not import or export, an exception is raised.
        """
        operator = SqoopOperator(task_id='sqoop_job', dag=self.dag,
                                 cmd_type='invalid')
        with self.assertRaises(AirflowException):
            operator.execute({})

    def test_invalid_import_options(self):
        """
        Tests to verify if a user passes both a query and a table then an exception is raised.
        """
        import_query_and_table_configs = self._config.copy()
        import_query_and_table_configs['cmd_type'] = 'import'
        operator = SqoopOperator(
            task_id='sqoop_job',
            dag=self.dag,
            **import_query_and_table_configs
        )
        with self.assertRaises(AirflowException):
            operator.execute({})


if __name__ == '__main__':
    unittest.main()
