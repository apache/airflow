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
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from airflow.providers.clickhouse.sensors.clickhouse import ClickHouseSensor

dag = DAG(
    'example_clickhouse_operator',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
)

# [START howto_sensor_clickhouse]

sensor = ClickHouseSensor(
    task_id="clickhouse_sensor",
    sql="SELECT * FROM gettingstarted.clickstream where customer_id='customer1'",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# [END howto_sensor_clickhouse]

# [START howto_sensor_template_file_clickhouse]

sensor_tf = ClickHouseSensor(
    task_id="clickhouse_sensor_template",
    sql="search_one.sql",
    timeout=60,
    poke_interval=10,
    dag=dag,
)


# [END howto_sensor_template_file_clickhouse]


# [START howto_operator_clickhouse]

operator = ClickHouseOperator(
    task_id='clickhouse_operator',
    sql="SELECT * FROM gettingstarted.clickstream",
    dag=dag,
    result_processor=lambda cursor: print(cursor)
)

# [END howto_operator_clickhouse]

# [START howto_operator_clickhouse_with_database]

operator_with_database = ClickHouseOperator(
    task_id='clickhouse_operator_with_db',
    sql="SELECT * FROM clickstream",    database='gettingstarted',
    dag=dag,
    result_processor=lambda cursor: print(cursor)
)

# [END howto_operator_clickhouse_with_database]

# [START howto_operator_template_file_clickhouse]

operator_tf = ClickHouseOperator(
    task_id='clickhouse_operator_tf',
    sql="search_all.sql",
    dag=dag,
    result_processor=lambda cursor: print(cursor)
)

# [END howto_operator_template_file_clickhouse]

