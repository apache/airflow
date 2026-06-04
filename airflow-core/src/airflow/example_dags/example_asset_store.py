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
Example Dag that demonstrates using AIP-103 asset store to track a watermark across DAG runs.
The producer reads the last watermark, processes only new records, then
advances the watermark. The consumer is triggered by the asset event and
reads asset store to understand what the producer just loaded.

Asset store persists on the asset across runs — unlike task store which is
scoped to a single task instance. This replaces the common pattern of
storing watermarks in Airflow Variables, which have no asset-level scoping.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone

from airflow.sdk import DAG, Asset, task

ORDERS = Asset(name="orders/daily", uri="s3://warehouse/orders/daily")


def _fetch_records(since: str) -> list[dict]:
    """Simulate fetching records newer than `since`."""
    return [{"id": i} for i in range(random.randint(100, 5_000))]


with DAG(
    dag_id="example_asset_store_producer",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "asset-store"],
    doc_md=__doc__,
):

    @task(inlets=[ORDERS], outlets=[ORDERS])
    def load(asset_store=None):
        state = asset_store[ORDERS]

        watermark = state.get("watermark", default="2026-01-01T00:00:00+00:00")
        records = _fetch_records(since=watermark)
        row_count = len(records)

        now = datetime.now(tz=timezone.utc).isoformat()
        state.set("watermark", now)
        state.set("total_runs", state.get("total_runs", default=0) + 1)
        state.set(
            "last_run_summary",
            {
                "rows_loaded": row_count,
                "prev_watermark": watermark,
                "completed_at": now,
            },
        )

        print(f"Loaded {row_count} records. Watermark advanced to {now}.")
        return row_count

    load()


with DAG(
    dag_id="example_asset_store_consumer",
    schedule=[ORDERS],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "asset-store"],
):

    @task(inlets=[ORDERS])
    def consume(asset_store=None):
        state = asset_store[ORDERS]
        summary = state.get("last_run_summary") or {}
        print(
            f"Processing {summary.get('rows_loaded', '?')} rows "
            f"up to watermark {state.get('watermark')}. "
            f"Total runs so far: {state.get('total_runs')}."
        )

    consume()
