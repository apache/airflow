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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.magento.operators.sales import GetOrdersOperator


def process_orders(**kwargs):
    ti = kwargs["ti"]
    orders = ti.xcom_pull(task_ids="get_orders_task", key="magento_orders")
    if orders:
        # Process orders
        for order in orders:
            print(order)
    else:
        print("No orders to process")


dag = DAG(
    dag_id="magento_orders_dag",
    start_date=datetime(2024, 8, 18),
    schedule_interval="@daily",
)

get_orders_task = GetOrdersOperator(task_id="get_orders_task", dag=dag, status="tt")

process_orders_task = PythonOperator(
    task_id="process_orders_task",
    python_callable=process_orders,
    provide_context=True,
    dag=dag,
)

get_orders_task >> process_orders_task
