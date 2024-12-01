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
This is an example DAG which uses RedisPublishOperator, RedisPubSubSensor and RedisKeySensor.
In this example, we create 3 tasks which execute sequentially.
The first task is to publish a particular message to redis using the RedisPublishOperator.
The second task is to wait for a particular message at a particular channel to arrive in redis
using the RedisPubSubSensor, and the third task is to wait for a particular key to arrive in
redis using the RedisKeySensor.

"""

from __future__ import annotations

import os
from datetime import datetime

# [START import_module]
from airflow import DAG
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.providers.redis.sensors.redis_key import RedisKeySensor

# [END import_module]
# [START instantiate_dag]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

default_args = {
    "start_date": datetime(2023, 5, 15),
    "max_active_runs": 1,
}

with DAG(
    dag_id="redis_example",
    schedule=None,
    default_args=default_args,
) as dag:
    # [START RedisPublishOperator_DAG]
    publish_task = RedisPublishOperator(
        task_id="publish_task",
        redis_conn_id="redis_default",
        channel="your_channel",
        message="Start processing",
        dag=dag,
    )

    # [END RedisPublishOperator_DAG]

    # [START RedisKeySensor_DAG]
    key_sensor_task = RedisKeySensor(
        task_id="key_sensor_task",
        redis_conn_id="redis_default",
        key="your_key",
        dag=dag,
        timeout=600,
        poke_interval=30,
    )
    # [END RedisKeySensor_DAG]

    publish_task >> key_sensor_task

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
