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
Report on each day of orders that the ETL service loads, through an asset route.

The Dag knows nothing of webhooks: it schedules on a data asset by day. The connection of the ``etl``
webhook has an asset route that records an event on ``s3://warehouse/orders`` for each
``load.completed`` delivery about the ``orders`` table, with the delivery's ``data.date`` as the event's
partition key, so each loaded day starts one run with that partition key.
"""

from __future__ import annotations

from airflow.sdk import Asset, PartitionedAssetTimetable, dag, task

orders = Asset(name="orders", uri="s3://warehouse/orders")


@dag(schedule=PartitionedAssetTimetable(assets=orders), catchup=False)
def orders_by_day():
    @task
    def report(**context):
        print(f"Reporting on the orders of {context.get('partition_key')}")

    report()


orders_by_day()
