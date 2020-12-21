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

"""Smart sensor DAGs managing all smart sensor tasks."""
from datetime import timedelta

from airflow.configuration import conf
from airflow.models import DAG
from airflow.sensors.smart_sensor import SmartSensorOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

num_smart_sensor_shard = conf.getint("smart_sensor", "shards")
shard_code_upper_limit = conf.getint('smart_sensor', 'shard_code_upper_limit')

for i in range(num_smart_sensor_shard):
    shard_min = (i * shard_code_upper_limit) / num_smart_sensor_shard
    shard_max = ((i + 1) * shard_code_upper_limit) / num_smart_sensor_shard

    dag_id = f'smart_sensor_group_shard_{i}'
    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        schedule_interval=timedelta(minutes=5),
        concurrency=1,
        max_active_runs=1,
        catchup=False,
        dagrun_timeout=timedelta(hours=24),
        start_date=days_ago(2),
    )

    SmartSensorOperator(
        task_id='smart_sensor_task',
        dag=dag,
        retries=100,
        retry_delay=timedelta(seconds=10),
        priority_weight=999,
        shard_min=shard_min,
        shard_max=shard_max,
        poke_timeout=10,
        smart_sensor_timeout=timedelta(hours=24).total_seconds(),
    )

    globals()[dag_id] = dag
