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
"""Example DAG for an async task that checkpoints its progress with ``aget``/``aset``.

The task fetches pages concurrently and records which ones finished. A staged failure
part way through the first attempt shows the retry resuming from the checkpoint instead
of re-fetching everything, and the Dag run still ends successfully.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.context import NEVER_EXPIRE

log = logging.getLogger(__name__)

PAGES = list(range(1, 13))
BATCH_SIZE = 4  # pages awaited concurrently before each checkpoint
CRASH_AFTER = 6  # staged failure, first attempt only


async def _fetch_page(page: int) -> int:
    """Placeholder for an awaited API call; returns the row count for the page."""
    await asyncio.sleep(0.2)
    return page * 100


with DAG(
    dag_id="example_task_state_store_async",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "task-state-store"],
    doc_md=__doc__,
) as dag:

    @task(retries=2, retry_delay=5)
    async def ingest_pages(task_state_store=None, ti=None) -> dict:
        """Fetch every page, checkpointing after each concurrent batch."""
        # Progress and the running total live in one key so a single write keeps
        # them consistent with each other.
        progress = await task_state_store.aget("progress", default={"done": [], "rows": 0})
        done = set(progress["done"])
        rows = progress["rows"]

        if done:
            log.info("Resuming: %d of %d pages already fetched", len(done), len(PAGES))
        else:
            log.info("Starting from the top: %d pages to fetch", len(PAGES))

        remaining = [page for page in PAGES if page not in done]

        for start in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[start : start + BATCH_SIZE]
            rows += sum(await asyncio.gather(*(_fetch_page(page) for page in batch)))
            done.update(batch)

            # Only this coroutine writes the checkpoint. If each _fetch_page wrote its
            # own, the writes would interleave at their await points, each overwriting a
            # stale copy of the set, and finished pages would vanish from the checkpoint.
            await task_state_store.aset(
                "progress",
                {"done": sorted(done), "rows": rows},
                retention=NEVER_EXPIRE,
            )
            log.info("Checkpointed %d/%d pages after batch %s", len(done), len(PAGES), batch)

            if CRASH_AFTER and ti.try_number == 1 and len(done) >= CRASH_AFTER:
                raise RuntimeError(
                    f"Staged worker loss after {len(done)} pages. The retry picks up from the checkpoint."
                )

        log.info("All %d pages fetched, %d rows total", len(done), rows)
        return {"pages": len(done), "rows": rows}

    ingest_pages()
