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

import unittest

import pytest

from airflow import DAG
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


@pytest.mark.integration("cassandra")
class TestCassandraHook(unittest.TestCase):
    def setUp(self) -> None:
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)

    def test_poke(self):
        hook = CassandraHook("cassandra_default_with_schema")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS s.t",
            "CREATE TABLE s.t (pk1 text PRIMARY KEY)",
        ]
        for cql in cqls:
            session.execute(cql)

        true_sensor = CassandraTableSensor(
            dag=self.dag,
            task_id="test_task",
            cassandra_conn_id="cassandra_default",
            table="s.t"
        )
        self.assertTrue(true_sensor.poke({}))

        true_sensor = CassandraTableSensor(
            dag=self.dag,
            task_id="test_task",
            cassandra_conn_id="cassandra_default",
            table="s.u"
        )
        self.assertFalse(true_sensor.poke({}))

        session.shutdown()
        hook.shutdown_cluster()
