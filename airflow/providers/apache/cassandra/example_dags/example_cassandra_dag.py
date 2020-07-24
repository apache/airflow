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

"""
Example Airflow DAG to check if a Cassandra Table and a Records exists
or not using `CassandraTableSensor` and `CassandraRecordSensor`.
"""
from airflow.models import DAG
from airflow.providers.apache.cassandra.sensors.record import CassandraRecordSensor
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_cassandra_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example']
) as dag:
    # [START howto_operator_cassandra_table_sensor]
    table_sensor = CassandraTableSensor(
        task_id="cassandra_table_sensor",
        cassandra_conn_id="cassandra_default",
        table="keyspace_name.table_name",
    )
    # [END howto_operator_cassandra_table_sensor]

    # [START howto_operator_cassandra_record_sensor]
    record_sensor = CassandraRecordSensor(
        task_id="cassandra_record_sensor",
        cassandra_conn_id="cassandra_default",
        table="keyspace_name.table_name",
        keys={"p1": "v1", "p2": "v2"},
    )
    # [END howto_operator_cassandra_record_sensor]
