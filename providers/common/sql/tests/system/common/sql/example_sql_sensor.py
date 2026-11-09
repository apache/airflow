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
from __future__ import annotations

from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor

with DAG(dag_id="example_sql_sensor", schedule=None, tags=["example"]) as dag:
    # [START howto_sensor_sql]
    wait_for_orders = SqlSensor(
        task_id="wait_for_orders",
        conn_id="sales_db",
        sql="SELECT COUNT(*) FROM orders WHERE order_date = CURRENT_DATE",
        poke_interval=300,
        mode="reschedule",
    )
    # [END howto_sensor_sql]

    # [START howto_sensor_sql_deferrable]
    wait_for_orders_deferrable = SqlSensor(
        task_id="wait_for_orders_deferrable",
        conn_id="sales_db",
        sql="SELECT COUNT(*) >= %(min_orders)s FROM orders WHERE order_date = CURRENT_DATE",
        parameters={"min_orders": 100},
        poke_interval=300,
        timeout=6 * 60 * 60,
        deferrable=True,
    )
    # [END howto_sensor_sql_deferrable]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
