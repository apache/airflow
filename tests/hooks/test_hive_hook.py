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
import random
import unittest

from hmsclient import HMSClient

from airflow.exceptions import AirflowException
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow import DAG, configuration, operators
from airflow.utils import timezone


configuration.load_test_config()


DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class HiveEnvironmentTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)
        self.next_day = (DEFAULT_DATE +
                         datetime.timedelta(days=1)).isoformat()[:10]
        self.database = 'airflow'
        self.partition_by = 'ds'
        self.table = 'static_babynames_partitioned'
        self.hql = """
        CREATE DATABASE IF NOT EXISTS {{ params.database }};
        USE {{ params.database }};
        DROP TABLE IF EXISTS {{ params.table }};
        CREATE TABLE IF NOT EXISTS {{ params.table }} (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY ({{ params.partition_by }} string);
        ALTER TABLE {{ params.table }}
        ADD PARTITION({{ params.partition_by }}='{{ ds }}');
        """
        self.hook = HiveMetastoreHook()
        t = operators.hive_operator.HiveOperator(
            task_id='HiveHook_' + str(random.randint(1, 10000)),
            params={
                'database': self.database,
                'table': self.table,
                'partition_by': self.partition_by
            },
            hive_cli_conn_id='beeline_default',
            hql=self.hql, dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
              ignore_ti_state=True)

    def tearDown(self):
        hook = HiveMetastoreHook()
        with hook.get_conn() as metastore:
            metastore.drop_table(self.database, self.table, deleteData=True)


class TestHiveMetastoreHook(HiveEnvironmentTest):
    VALID_FILTER_MAP = {'key2': 'value2'}

    def test_get_max_partition_from_empty_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs([],
                                                                 'key1',
                                                                 self.VALID_FILTER_MAP)
        self.assertIsNone(max_partition)

    def test_get_max_partition_from_valid_part_specs_and_invalid_filter_map(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                {'key3': 'value5'})

    def test_get_max_partition_from_valid_part_specs_and_invalid_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key3',
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                None,
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_filter_map(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                None)

        # No partition will be filtered out.
        self.assertEqual(max_partition, b'value3')

    def test_get_max_partition_from_valid_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                self.VALID_FILTER_MAP)
        self.assertEqual(max_partition, b'value1')

    def test_get_metastore_client(self):
        self.assertIsInstance(self.hook.get_metastore_client(), HMSClient)

    def test_get_conn(self):
        self.assertIsInstance(self.hook.get_conn(), HMSClient)

    def test_check_for_partition(self):
        partition = "{p_by}='{date}'".format(date=DEFAULT_DATE_DS,
                                             p_by=self.partition_by)
        missing_partition = "{p_by}='{date}'".format(date=self.next_day,
                                                     p_by=self.partition_by)
        self.assertTrue(
            self.hook.check_for_partition(self.database, self.table,
                                          partition)
        )
        self.assertFalse(
            self.hook.check_for_partition(self.database, self.table,
                                          missing_partition)
        )

    def test_check_for_named_partition(self):
        partition = "{p_by}={date}".format(date=DEFAULT_DATE_DS,
                                           p_by=self.partition_by)
        missing_partition = "{p_by}={date}".format(date=self.next_day,
                                                   p_by=self.partition_by)
        self.assertTrue(
            self.hook.check_for_named_partition(self.database,
                                                self.table,
                                                partition)
        )
        self.assertFalse(
            self.hook.check_for_named_partition(self.database,
                                                self.table,
                                                missing_partition)
        )

    def test_get_table(self):
        table_info = self.hook.get_table(db=self.database,
                                         table_name=self.table)
        self.assertEqual(table_info.tableName, self.table)
        columns = ['state', 'year', 'name', 'gender', 'num']
        self.assertEqual([col.name for col in table_info.sd.cols], columns)

    def test_get_tables(self):
        tables = self.hook.get_tables(db=self.database,
                                      pattern=self.table + "*")
        self.assertIn(self.table, {table.tableName for table in tables})

    def get_databases(self):
        databases = self.hook.get_databases(pattern='*')
        self.assertIn(self.database, databases)

    def test_get_partitions(self):
        partitions = self.hook.get_partitions(schema=self.database,
                                              table_name=self.table)
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions, [{self.partition_by: DEFAULT_DATE_DS}])

    def test_max_partition(self):
        filter_map = {self.partition_by: DEFAULT_DATE_DS}
        partition = self.hook.max_partition(schema=self.database,
                                            table_name=self.table,
                                            field=self.partition_by,
                                            filter_map=filter_map)
        self.assertEqual(partition, DEFAULT_DATE_DS.encode('utf-8'))

    def test_table_exists(self):
        self.assertTrue(self.hook.table_exists(self.table, db=self.database))
        self.assertFalse(
            self.hook.table_exists(str(random.randint(1, 10000)))
        )
