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
Example DAGs that write demo column lineage into AssetEvent.extra.

Run the DAGs in this order to generate demo data for the Asset Only lineage view:

1. ``asset_column_lineage_source``
2. ``asset_column_lineage_curated``
3. ``asset_column_lineage_metrics``
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, Asset, task

raw_orders = Asset(uri="warehouse://raw/orders", name="raw_orders")
curated_orders = Asset(uri="warehouse://curated/orders", name="curated_orders")
customer_metrics = Asset(uri="warehouse://metrics/customer_orders", name="customer_order_metrics")


with DAG(
    dag_id="asset_column_lineage_source",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["assets", "lineage-demo", "column-lineage"],
):

    @task(outlets=[raw_orders])
    def produce_raw_orders():
        pass

    produce_raw_orders()


with DAG(
    dag_id="asset_column_lineage_curated",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["assets", "lineage-demo", "column-lineage"],
):

    @task(inlets=[raw_orders], outlets=[curated_orders])
    def build_curated_orders(*, outlet_events=None):
        outlet_events[curated_orders].extra = {
            "column_lineage": {
                "order_id": [
                    {
                        "source_asset_uri": raw_orders.uri,
                        "source_column": "id",
                    }
                ],
                "customer_id": [
                    {
                        "source_asset_uri": raw_orders.uri,
                        "source_column": "customer_id",
                    }
                ],
                "total_amount": [
                    {
                        "source_asset_uri": raw_orders.uri,
                        "source_column": "amount",
                    }
                ],
            }
        }

    build_curated_orders()


with DAG(
    dag_id="asset_column_lineage_metrics",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["assets", "lineage-demo", "column-lineage"],
):

    @task(inlets=[curated_orders], outlets=[customer_metrics])
    def build_customer_order_metrics(*, outlet_events=None):
        outlet_events[customer_metrics].extra = {
            "column_lineage": {
                "customer_id": [
                    {
                        "source_asset_uri": curated_orders.uri,
                        "source_column": "customer_id",
                    }
                ],
                "order_count": [
                    {
                        "source_asset_uri": curated_orders.uri,
                        "source_column": "order_id",
                    }
                ],
                "gross_revenue": [
                    {
                        "source_asset_uri": curated_orders.uri,
                        "source_column": "total_amount",
                    }
                ],
            }
        }

    build_customer_order_metrics()
