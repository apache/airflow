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
import random
import unittest
from datetime import timedelta

from airflow import DAG, operators
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class TestNamedHivePartitionSensor(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)
        self.next_day = (DEFAULT_DATE +
                         timedelta(days=1)).isoformat()[:10]
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
        op = operators.hive_operator.HiveOperator(
            task_id='HiveHook_' + str(random.randint(1, 10000)),
            params={
                'database': self.database,
                'table': self.table,
                'partition_by': self.partition_by
            },
            hive_cli_conn_id='hive_cli_default',
            hql=self.hql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def tearDown(self):
        hook = HiveMetastoreHook()
        with hook.get_conn() as metastore:
            metastore.drop_table(self.database, self.table, deleteData=True)

    def test_parse_partition_name_correct(self):
        schema = 'default'
        table = 'users'
        partition = 'ds=2016-01-01/state=IT'
        name = '{schema}.{table}/{partition}'.format(schema=schema,
                                                     table=table,
                                                     partition=partition)
        parsed_schema, parsed_table, parsed_partition = (
            NamedHivePartitionSensor.parse_partition_name(name)
        )
        self.assertEqual(schema, parsed_schema)
        self.assertEqual(table, parsed_table)
        self.assertEqual(partition, parsed_partition)

    def test_parse_partition_name_incorrect(self):
        name = 'incorrect.name'
        with self.assertRaises(ValueError):
            NamedHivePartitionSensor.parse_partition_name(name)

    def test_parse_partition_name_default(self):
        table = 'users'
        partition = 'ds=2016-01-01/state=IT'
        name = '{table}/{partition}'.format(table=table,
                                            partition=partition)
        parsed_schema, parsed_table, parsed_partition = (
            NamedHivePartitionSensor.parse_partition_name(name)
        )
        self.assertEqual('default', parsed_schema)
        self.assertEqual(table, parsed_table)
        self.assertEqual(partition, parsed_partition)

    def test_poke_existing(self):
        partitions = ["{}.{}/{}={}".format(self.database,
                                           self.table,
                                           self.partition_by,
                                           DEFAULT_DATE_DS)]
        sensor = NamedHivePartitionSensor(partition_names=partitions,
                                          task_id='test_poke_existing',
                                          poke_interval=1,
                                          hook=self.hook,
                                          dag=self.dag)
        self.assertTrue(sensor.poke(None))

    def test_poke_non_existing(self):
        partitions = ["{}.{}/{}={}".format(self.database,
                                           self.table,
                                           self.partition_by,
                                           self.next_day)]
        sensor = NamedHivePartitionSensor(partition_names=partitions,
                                          task_id='test_poke_non_existing',
                                          poke_interval=1,
                                          hook=self.hook,
                                          dag=self.dag)
        self.assertFalse(sensor.poke(None))
