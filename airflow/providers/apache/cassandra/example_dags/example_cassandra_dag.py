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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

"""
Example Airflow DAG to check if a Cassandra Table and a Records exists
or not using `CassandraTableSensor` and `CassandraRecordSensor`.
"""
from datetime import datetime

from airflow.models import DAG
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor

# [START howto_operator_cassandra_sensors]
with DAG(
    dag_id='example_cassandra_operator',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={'table': 'keyspace_name.table_name'},
    catchup=False,
    tags=['example'],
) as dag:
    table_sensor = CassandraTableSensor(task_id="cassandra_table_sensor")

    record_sensor = CassandraRecordSensor(task_id="cassandra_record_sensor", keys={"p1": "v1", "p2": "v2"})
# [END howto_operator_cassandra_sensors]
