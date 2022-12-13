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

import os

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
) as dag:
    dynamodb_sensor = DynamoDBValueSensor(
        task_id="waiting_for_dynamodb_item_value",
        poke_interval=30,
        timeout=120,
        soft_fail=False,
        retries=10,
        table_name="AirflowSensorTest", # replace with your table name
        partition_key_name="PK", # replace with your partition key name
        partition_key_value="Test", # replace with your partition key value
        sort_key_name="SK", # replace with your sort key name (if applicable)
        sort_key_value="2022-07-12T11:11:25-0400", # replace with your sort key value (if applicable)
        attribute_name="Foo", # replace with the attribute name to wait for
        attribute_value="Bar", # replace with the attribute value to wait for (sensor will return true when this value matches the attribute value in the item)
    )
