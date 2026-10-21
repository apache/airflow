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
[DO NOT MERGE] AIP-125 spike S10: what copying webhook payloads into ``asset_event.extra`` costs.

For each payload size, records ``--deliveries`` deliveries the way the delivery pipeline records topic
events: one transaction per delivery, which records an event on each of ``--names`` assets with
``AssetManager.register_asset_change`` and the extra that ``build_event_extra`` builds. Each delivery gets
a payload of its own, and its events share it, as the events of one delivery do. It prints the insert
time per delivery, the growth of ``asset_event`` with its TOAST table and indexes, the growth of its GIN
index on ``extra``, and the latency of ``GET /api/v2/assets/events``. Only PostgreSQL has the GIN index,
and only a schema built by the migrations, which ``demo.sh`` does with ``airflow db reset
--use-migration-files``: migration 0124 creates it, while a schema created from the ORM has none.

At these table sizes PostgreSQL scans ``asset_event`` for the API's filter on ``extra`` rather than use the
GIN index, because its cost model does not count TOAST, so ``GET by extra`` times a scan that reads and
casts every event's extra. The ``by extra plan`` column names the scan the planner chose, and
``by extra, GIN`` times the same filter with sequential scans turned off, which is the indexed lookup.

Payloads are copied whatever their size unless ``--max-event-payload-bytes`` is set, so that the cost
that ``[webhooks] max_event_payload_bytes`` (65536 by default) caps is visible. The delivery records,
which keep the payload once whatever the number of names, are not written.

It needs the database and the core API server of ``demo.sh``, so run it through ``demo.sh bench``, which
passes it the options in ``BENCH_PAYLOAD_ARGS``::

    breeze run --backend postgres bash -c \
        "BENCH_PAYLOAD_ARGS='--max-event-payload-bytes 65536' bash /opt/airflow/dev/aip125_demo/demo.sh bench"
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import time
import uuid
from typing import TYPE_CHECKING, Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy import delete, select, text

from airflow.assets.manager import asset_manager
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from airflow.providers.standard.webhooks.generic import HmacJsonSource
from airflow.serialization.definitions.assets import SerializedAssetUniqueKey
from airflow.utils.session import create_session
from airflow.webhooks.base import WebhookDelivery
from airflow.webhooks.connections import ConfiguredWebhook
from airflow.webhooks.topics import build_event_extra

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

GIN_INDEX = "idx_asset_event_extra_gin"
# The filter that GET /api/v2/assets/events?extra=key=value runs on PostgreSQL, which the GIN index serves.
EXTRA_FILTER = "SELECT count(*) FROM asset_event WHERE CAST(extra AS JSONB) @> CAST(:contained AS JSONB)"
NAME_PREFIX = "aip125-bench/"
SIZES = {"1 KiB": 1024, "64 KiB": 65536, "1 MiB": 1048576}
WORDS = "add fix bump drop move test docs build cache index query table event asset webhook".split()
USERS = ["octocat", "hubot", "monalisa", "dependabot"]


def build_payload(size: int, rng: random.Random) -> dict[str, Any]:
    """
    Return a push-like payload of compact JSON just under ``size`` bytes: one more commit would cross it.

    It stays within ``size`` so that a payload of ``max_event_payload_bytes`` is copied, not truncated.
    Ids and messages are random, so that it compresses about as well as a real one.
    """
    commits: list[dict[str, Any]] = []
    payload = {"ref": "refs/heads/main", "repository": {"full_name": "my-org/web"}, "commits": commits}
    length = len(json.dumps(payload, separators=(",", ":")))
    while True:
        sha = f"{rng.getrandbits(160):040x}"
        commit = {
            "id": sha,
            "message": " ".join(rng.choices(WORDS, k=rng.randint(4, 16))),
            "url": f"https://github.com/my-org/web/commit/{sha}",
            "author": {"username": rng.choice(USERS)},
            "modified": [f"src/{rng.choice(WORDS)}/{rng.choice(WORDS)}.py" for _ in range(rng.randint(1, 4))],
        }
        added = len(json.dumps(commit, separators=(",", ":"))) + (1 if commits else 0)
        if length + added > size:
            return payload
        commits.append(commit)
        length += added


def build_bench_webhook() -> ConfiguredWebhook:
    source = HmacJsonSource.from_extra({"signature_header": "X-Signature", "event_field": "event"})
    return ConfiguredWebhook(
        name="bench",
        source_type="hmac_json",
        source=source,
        secret="unused",
        max_body_bytes=2 * max(SIZES.values()),
        store_payload="full",
    )


def delete_bench_assets(*, session: Session) -> None:
    asset_ids = select(AssetModel.id).where(AssetModel.name.startswith(NAME_PREFIX))
    for statement in (
        delete(AssetEvent).where(AssetEvent.asset_id.in_(asset_ids)),
        delete(AssetActive).where(AssetActive.name.startswith(NAME_PREFIX)),
        delete(AssetModel).where(AssetModel.name.startswith(NAME_PREFIX)),
    ):
        session.execute(statement.execution_options(synchronize_session=False))


def create_bench_assets(names: list[str], *, session: Session) -> None:
    for name in names:
        asset = AssetModel(name=name, uri=name, group="webhook")
        session.add(asset)
        session.flush()
        session.add(AssetActive.for_asset(asset))


def record_delivery(
    webhook: ConfiguredWebhook, names: list[str], payload: dict[str, Any], max_event_payload_bytes: int
) -> tuple[float, str, int]:
    """Record one delivery's topic events; return the seconds it took, its delivery id and extra size."""
    delivery_id = f"bench-{uuid.uuid4()}"
    delivery = WebhookDelivery(
        path=None,
        kinds=[("deployed",)],
        attributes={"event": "deployed"},
        delivery_id=delivery_id,
        payload=payload,
    )
    callbacks: list = []
    started = time.monotonic()
    extra = build_event_extra(
        webhook,
        delivery,
        inbound_delivery_id=uuid.uuid4(),
        payload=webhook.select_payload(delivery.payload),
        max_event_payload_bytes=max_event_payload_bytes,
    )
    with create_session(scoped=False) as session:
        for name in names:
            asset_manager.register_asset_change(
                asset=SerializedAssetUniqueKey(name=name, uri=name),
                extra=extra,
                partition_key=None,
                source_is_api=True,
                api_user_teams=set(),
                callback_sink=callbacks,
                session=session,
            )
    elapsed = time.monotonic() - started
    for callback in callbacks:
        callback()
    return elapsed, delivery_id, len(json.dumps(extra, separators=(",", ":")).encode())


def measure_storage(*, session: Session) -> tuple[int | None, int | None]:
    """Return the bytes of the GIN index on asset_event.extra and of asset_event with TOAST and indexes."""
    if session.get_bind().dialect.name != "postgresql":
        return None, None
    index_bytes = None
    if session.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": GIN_INDEX}):
        # GIN indexes take new entries into a pending list first. Merge it, so that the size is that of
        # the index once its entries are in the main structure.
        session.execute(text("SELECT gin_clean_pending_list(CAST(:name AS regclass))"), {"name": GIN_INDEX})
        index_bytes = session.scalar(
            text("SELECT pg_relation_size(CAST(:name AS regclass))"), {"name": GIN_INDEX}
        )
    return index_bytes, session.scalar(text("SELECT pg_total_relation_size('asset_event')"))


def find_scans(plan: dict[str, Any]) -> str:
    """Return the scans of an ``EXPLAIN (FORMAT JSON)`` plan, outermost first, such as heap and index."""
    scans = []
    pending = [plan]
    while pending:
        node = pending.pop(0)
        if "Scan" in node["Node Type"]:
            scans.append(node["Node Type"])
        pending.extend(node.get("Plans", []))
    return " + ".join(scans) or "no scan"


def explain_extra_filter(delivery_id: str, *, session: Session) -> tuple[str, str]:
    """
    Return the scan that the API's filter on ``extra`` runs as, and the indexed lookup's time.

    The second ``EXPLAIN ANALYZE`` runs with sequential scans turned off for this transaction only.
    """
    if session.get_bind().dialect.name != "postgresql":
        return "n/a", "n/a"
    explain = text(f"EXPLAIN (ANALYZE, FORMAT JSON) {EXTRA_FILTER}")
    params = {"contained": json.dumps({"delivery_id": delivery_id})}
    (chosen,) = session.scalar(explain, params)
    session.execute(text("SET LOCAL enable_seqscan = off"))
    (forced,) = session.scalar(explain, params)
    return find_scans(chosen["Plan"]), f"{find_scans(forced['Plan'])}, {forced['Execution Time']:.1f} ms"


def get_token(api_url: str, username: str, password: str) -> str | None:
    request = Request(
        f"{api_url}/auth/token",
        data=json.dumps({"username": username, "password": password}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)["access_token"]
    except (URLError, OSError, KeyError, ValueError) as e:
        print(f"No token from {api_url}/auth/token, so the API is not timed: {e}")
        return None


def time_get_events(api_url: str, token: str, query: dict[str, Any], repeat: int = 5) -> tuple[float, int]:
    """Return the median milliseconds of GET /api/v2/assets/events with ``query``, and the answer's size."""
    url = f"{api_url}/api/v2/assets/events?{urlencode(query)}"
    timings = []
    size = 0
    for _ in range(repeat):
        started = time.monotonic()
        with urlopen(Request(url, headers={"Authorization": f"Bearer {token}"}), timeout=120) as response:
            size = len(response.read())
        timings.append(time.monotonic() - started)
    return statistics.median(timings) * 1000, size


def format_bytes(value: float | None) -> str:
    if value is None:
        return "n/a"
    for unit in ("B", "KiB", "MiB"):
        if abs(value) < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--deliveries", type=int, default=10, help="deliveries per payload size")
    parser.add_argument("--names", type=int, default=9, help="topic events per delivery")
    parser.add_argument("--max-event-payload-bytes", type=int, default=0, help="0 copies every payload")
    parser.add_argument("--api-url", default="http://localhost:8080", help="the core API server")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="admin")
    parser.add_argument("--keep", action="store_true", help="keep the bench assets and their events")
    args = parser.parse_args()

    rng = random.Random(125)
    webhook = build_bench_webhook()
    names = [f"{NAME_PREFIX}service-{i}/deployed" for i in range(args.names)]
    with create_session(scoped=False) as session:
        delete_bench_assets(session=session)
        create_bench_assets(names, session=session)
    token = get_token(args.api_url, args.username, args.password)

    rows = []
    try:
        for label, size in SIZES.items():
            with create_session(scoped=False) as session:
                index_before, table_before = measure_storage(session=session)
            timings = []
            delivery_id: str | None = None
            extra_bytes: int | None = None
            for _ in range(args.deliveries):
                elapsed, delivery_id, extra_bytes = record_delivery(
                    webhook, names, build_payload(size, rng), args.max_event_payload_bytes
                )
                timings.append(elapsed * 1000)
            with create_session(scoped=False) as session:
                index_after, table_after = measure_storage(session=session)
            row = {
                "payload": label,
                "events": str(args.deliveries * args.names),
                "extra/event": format_bytes(extra_bytes),
                "insert p50": f"{statistics.median(timings):.1f} ms",
                "insert max": f"{max(timings):.1f} ms",
                "table+TOAST+indexes": format_bytes(
                    None if table_before is None or table_after is None else table_after - table_before
                ),
                "GIN index": format_bytes(
                    None if index_before is None or index_after is None else index_after - index_before
                ),
                "GET latest 50": "n/a",
                "GET by extra": "n/a",
                "by extra plan": "n/a",
                "by extra, GIN": "n/a",
            }
            if delivery_id:
                with create_session(scoped=False) as session:
                    row["by extra plan"], row["by extra, GIN"] = explain_extra_filter(
                        delivery_id, session=session
                    )
            if token:
                latest_ms, latest_bytes = time_get_events(
                    args.api_url, token, {"limit": 50, "order_by": "-timestamp"}
                )
                row["GET latest 50"] = f"{latest_ms:.0f} ms, {format_bytes(latest_bytes)}"
                by_extra_ms, _ = time_get_events(args.api_url, token, {"extra": f"delivery_id={delivery_id}"})
                row["GET by extra"] = f"{by_extra_ms:.0f} ms"
            rows.append(row)
            print(f"{label}: done")
    finally:
        if not args.keep:
            with create_session(scoped=False) as session:
                delete_bench_assets(session=session)

    print(
        f"\n{args.deliveries} deliveries per size, {args.names} topic events each, "
        f"max_event_payload_bytes={args.max_event_payload_bytes}; growth is per size\n"
    )
    if rows[0]["GIN index"] == "n/a" and rows[0]["table+TOAST+indexes"] != "n/a":
        print(
            f"There is no {GIN_INDEX}: the schema was created from the ORM rather than by the migrations.\n"
        )
    columns = list(rows[0])
    widths = {column: max(len(column), *(len(row[column]) for row in rows)) for column in columns}
    print("  ".join(column.ljust(widths[column]) for column in columns).rstrip())
    for row in rows:
        print("  ".join(row[column].ljust(widths[column]) for column in columns).rstrip())


if __name__ == "__main__":
    main()
