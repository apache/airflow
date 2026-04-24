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

from airflow.sdk import DAG, Asset, task

raw_orders = Asset(uri="file://cs5150/realtime/raw_orders.csv", name="cs5150_raw_orders")
clean_orders = Asset(uri="file://cs5150/realtime/clean_orders.csv", name="cs5150_clean_orders")
orders_report = Asset(uri="file://cs5150/realtime/orders_report.csv", name="cs5150_orders_report")


with DAG(
    dag_id="cs5150_realtime_extract_orders",
    schedule=None,
    tags=["cs5150", "lineage-demo"],
):
    """Produce the raw orders asset for the CS5150 lineage demo."""

    @task(outlets=[raw_orders])
    def extract_orders():
        print("Produced raw orders")

    extract_orders()


with DAG(
    dag_id="cs5150_realtime_clean_orders",
    schedule=None,
    tags=["cs5150", "lineage-demo"],
):
    """Consume raw orders and produce the cleaned orders asset."""

    @task(inlets=[raw_orders], outlets=[clean_orders])
    def clean_orders_task():
        print("Produced clean orders")

    clean_orders_task()


with DAG(
    dag_id="cs5150_realtime_publish_report",
    schedule=None,
    tags=["cs5150", "lineage-demo"],
):
    """Consume cleaned orders and produce a report asset."""

    @task(inlets=[clean_orders], outlets=[orders_report])
    def publish_orders_report():
        print("Produced orders report")

    publish_orders_report()
