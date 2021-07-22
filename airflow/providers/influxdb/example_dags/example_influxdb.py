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

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from airflow.utils.dates import days_ago


def test_influxdb_hook():
    bucket_name = 'test-influx'
    influxdb_hook = InfluxDBHook("influxdb_default")
    client = influxdb_hook.get_conn()
    print(client)
    print(f"Organization name {influxdb_hook.org_name}")

    # Make sure enough permissions to create bucket.
    influxdb_hook.create_bucket(bucket_name, "Bucket to test influxdb connection", influxdb_hook.org_name)
    influxdb_hook.write(bucket_name, "test_point", "location", "Prague", "temperature", 25.3, True)

    tables = influxdb_hook.query('from(bucket:"test-influx") |> range(start: -10m)')

    for table in tables:
        print(table)
        for record in table.records:
            print(record.values)

    bucket_id = influxdb_hook.find_bucket_id_by_name(bucket_name)
    print(bucket_id)
    # Delete bucket takes bucket id.
    influxdb_hook.delete_bucket(bucket_name)


with DAG(
    dag_id='influxdb_example_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:
    influxdb_task = PythonOperator(task_id="influxdb_task", python_callable=test_influxdb_hook)

    influxdb_task
