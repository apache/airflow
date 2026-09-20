<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Asset-state-store conversion evidence

Use this example when auditing a backend getter's projection or benchmarking CPU work that grows with one JSON value. The measurements concern [PR #74540](https://github.com/apache/airflow/pull/74540) at `a0d2de8247d7a7cdeb4a46e17d14caaf4ce4e0db`, compared with `fe589528ee46f9afdc2060ef14452a855cfbab30`. These are observations from that checkout, not current API guarantees or performance thresholds.

## Query width

Both asset-name and URI resolvers already select only `AssetModel.id`. Task writer attribution selects eight fields and uses all eight. The reused backend getter `MetastoreBackend._aget_asset_state_store`, however, selects the full `AssetStateStoreModel` and returns only `row.value`: 13 selected columns for one consumed value, with no relationship or model method needed. A scalar `value` projection preserves its missing-key result because the stored value is non-null. The synchronous counterpart has the same opportunity, but is not adopted by this async conversion; keep that distinction when bounding review findings.

A comparative HTTP probe captured the selected columns and ORM `load` events on SQLite, PostgreSQL, and MySQL. Per backend and variant it exercised both selectors with six values (`false`, zero, empty string/object/list, and a nested object), plus a missing key. The existing getter selected 13 columns and hydrated 12 entities for the 12 successful reads; the value-only getter selected one column and hydrated none, preserving all 14 HTTP outcomes. This proves narrower retrieval and avoided hydration.

For a 390-byte value, matched timing samples measured warm getter medians of 0.816 → 0.774 ms on SQLite, 0.662 → 0.586 ms on PostgreSQL, and 1.074 → 0.801 ms on MySQL. Each variant had 400 reads in interleaved pairs over two rounds, on a shared warm async connection. The absolute savings were 0.04–0.27 ms; full HTTP latency had no consistent improvement across selectors, rounds, and payload sizes. Narrower retrieval does not by itself establish a material endpoint speedup.

## JSON CPU work

The GET handlers construct `AssetStateStoreResponse(value=json.loads(value))`. Converting them to `async def` moves parsing and recursive model validation from FastAPI's worker thread to the event loop. A stored 100,000-integer array was one row but 688,890 JSON bytes. Two SQLite rounds measured these warm median loop gaps:

| Variant | By-name GET | By-URI GET |
|---|---|---|
| Synchronous base | 110 / 113 ms | 121 / 138 ms |
| Async conversion | 152 / 155 ms | 149 / 149 ms |
| Parsing and model construction offloaded together | 129 / 112 ms | 130 / 131 ms |

Each scenario used eleven sequential real HTTP-stack requests through TestClient, discarded the first, and measured each 1 ms ticker sleep on the server loop. The client request ran in another thread. Profiles used separate requests. Maximum stalls were variable; the evidence supports a repeated median scheduling cost, not a uniform maximum-stall or throughput claim. PUT encoding offload leaves median stalls above the base; the remaining framework costs lack an isolated remedy.

The loop profile included an additional 100,001 Pydantic type-name calls on the async GET path. Offloading parsing and model construction removed that additional work from the loop profile, but framework serialization still caused substantial stalls. Profile totals did not measure baseline worker-thread CPU.

The actual runtime was Python 3.11.16: this checkout's Breeze rejected requested 3.15 and fell back to its default. Clean comparisons omitted the fixture's fake Authorization header while retaining its independent auth override, avoiding unrelated token-reissue exceptions. Contract tests passed on SQLite, PostgreSQL, and MySQL for base and head; performance samples used SQLite only.
