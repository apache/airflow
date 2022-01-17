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

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.influxdb.operators.influxdb import InfluxDBOperator

dag = DAG(
    'example_influxdb_operator',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
)

# [START howto_operator_influxdb]

query_influxdb_task = InfluxDBOperator(
    influxdb_conn_id='influxdb_conn_id',
    task_id='query_influxdb',
    sql='from(bucket:"test-influx") |> range(start: -10m, stop: {{ds}})',
    dag=dag,
)

# [END howto_operator_influxdb]
