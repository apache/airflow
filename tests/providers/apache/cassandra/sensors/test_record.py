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
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


@pytest.mark.integration("cassandra")
class TestCassandraRecordSensor(unittest.TestCase):
    def setUp(self) -> None:
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)

    def test_poke(self):
        hook = CassandraHook(cassandra_conn_id="cassandra_default")
        session = hook.get_conn()
        cqls = [
            "DROP TABLE IF EXISTS s.t",
            "CREATE TABLE s.t (pk1 text, pk2 text, c text, PRIMARY KEY (pk1, pk2))",
            "INSERT INTO s.t (pk1, pk2, c) VALUES ('foo', 'bar', 'baz')",
        ]
        for cql in cqls:
            session.execute(cql)

        true_sensor = CassandraRecordSensor(
            dag=self.dag,
            task_id="test_task",
            cassandra_conn_id="cassandra_default",
            table="s.t",
            keys={"pk1": "foo", "pk2": "bar"}
        )
        self.assertTrue(true_sensor.poke({}))

        false_sensor = CassandraRecordSensor(
            dag=self.dag,
            task_id="test_task",
            cassandra_conn_id="cassandra_default",
            table="s.u",
            keys={"pk1": "foo", "pk2": "baz"}
        )
        self.assertFalse(false_sensor.poke({}))

        session.shutdown()
        hook.shutdown_cluster()
