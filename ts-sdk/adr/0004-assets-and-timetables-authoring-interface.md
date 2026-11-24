<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# ADR-0004: TypeScript Authoring Interface for Assets and Timetables

## Status

Proposed.

## Context

The shared contract for assets, timetables and Python references is in
[`airflow-core/adr/lang-sdk/0019`](../../airflow-core/adr/lang-sdk/0019-assets-and-timetables-across-lang-sdks.md).
This ADR records the TypeScript spelling of it.

## Decision

1. **Assets are classes taking an options object.** Refs come from `Asset.ref({ name })` or
   `Asset.ref({ uri })`. Conditions use `.and()`, `.or()`, `Asset.all()` and `Asset.any()`, since
   TypeScript has no operator overloading.
2. **Builders live in modules used as namespaces.** `package.json` `exports` gains `./schedules`,
   `./mappers` and `./python`. `Timetable`, `PartitionMapper`, `Window`, `WaitPolicy` and
   `TriggerRef` are opaque types exported from the package root.
3. **`DagSpec.schedule` becomes `string | Timetable`.** The generated field is widened in the
   generator, so it stays `readonly`. A string keeps its current meaning.
4. **`inlets` and `outlets` are `TaskSpec` fields.** The schema generator stops excluding them, so
   `TaskSpec` accepts them.
5. **Outlet and triggering events are on `getContext()`; inlet events and the asset state store
   are on `getClient()`.** Outlet events are set through methods. `assetStateStore(asset)` is a
   method, unlike the `taskStateStore` property, because it takes the asset. It mirrors
   `TaskStateStore` without the retention option, which the asset messages do not carry.
6. **Inlet event methods take an `InletEventQueryParams` object** and return promises.
7. **`AssetExtra` aliases `JsonObject`** for asset and event `extra`.
8. **Durations are seconds as a `number`; datetimes are a `Date` or an ISO 8601 string with an
   offset.** A string without an offset is rejected at build time.

## Signatures

```ts
// @apache-airflow/ts-sdk
type JsonObject = { [key: string]: JsonValue };
type AssetExtra = JsonObject;
type AssetLike = Asset | AssetRef | AssetAlias;
type AssetExpression = AssetLike | AssetCondition;

class Asset {
  constructor(options: {
    name?: string;                     // one of name and uri is required; the other defaults to it
    uri?: string;
    group?: string;                    // default "asset"
    extra?: AssetExtra;
    watchers?: AssetWatcher[];
    accessControl?: {                  // omitted fields take Python's defaults
      producerTeams?: string[];        // []
      consumerTeams?: string[];        // unset
      allowGlobal?: boolean;           // true
    };
  });
  static ref(key: { name: string } | { uri: string }): AssetRef;
  static all(...items: AssetExpression[]): AssetCondition;
  static any(...items: AssetExpression[]): AssetCondition;
  and(...others: AssetExpression[]): AssetCondition;   // also on AssetRef, AssetAlias, AssetCondition
  or(...others: AssetExpression[]): AssetCondition;
}
class AssetAlias { constructor(options: { name: string; group?: string }); }   // group default "asset"
class AssetWatcher { constructor(options: { name: string; trigger: TriggerRef }); }  // a BaseEventTrigger

interface DagSpec { readonly schedule?: string | Timetable; /* ... */ }
interface TaskSpec { readonly inlets?: AssetLike[]; readonly outlets?: AssetLike[]; /* ... */ }

interface TaskContext {
  outletEvents(asset: AssetLike): OutletEventAccessor;
  triggeringAssetEvents(asset: AssetLike): readonly TriggeringAssetEvent[];
}
interface OutletEventAccessor {
  setExtra(extra: AssetExtra): void;
  addPartitions(keys: string | string[]): void;             // throws on an alias
  add(asset: Asset | AssetRef, extra?: AssetExtra): void;   // throws on a non-alias
}
interface TaskClient {
  inletEvents(asset: AssetLike): InletEventAccessor;
  assetStateStore(asset: Asset | AssetRef): AssetStateStore;
}
interface InletEventQueryParams {
  after?: Date | string;
  before?: Date | string;
  ascending?: boolean;                  // default true
  limit?: number;
  partitionKey?: string;
  partitionKeyPattern?: string;
  extra?: Record<string, string>;
}
type InletEventFilter = Omit<InletEventQueryParams, "ascending" | "limit">;
interface InletEventAccessor {
  query(params?: InletEventQueryParams): Promise<AssetEvent[]>;
  first(filter?: InletEventFilter): Promise<AssetEvent | null>;   // oldest
  last(filter?: InletEventFilter): Promise<AssetEvent | null>;    // newest
}
interface AssetStateStore {
  get<T = unknown>(key: string): Promise<T | null>;
  set(key: string, value: NonNullable<JsonValue>): Promise<void>;
  delete(key: string): Promise<void>;
  clear(): Promise<void>;
}
interface AssetEvent {                  // created_dagruns is not exposed
  id: number;
  timestamp: Date;
  extra: AssetExtra | null;
  asset: { name: string; uri: string; group: string; extra: AssetExtra | null };
  sourceDagId: string | null;
  sourceTaskId: string | null;
  sourceRunId: string | null;
  sourceMapIndex: number | null;
  partitionKey: string | null;
}
interface TriggeringAssetEvent {        // from consumed_asset_events
  timestamp: Date;
  extra: AssetExtra;
  asset: { name: string; uri: string; extra: AssetExtra };
  sourceDagId: string | null;
  sourceTaskId: string | null;
  sourceRunId: string | null;
  sourceMapIndex: number | null;
  sourceAliases: string[];
  partitionKey: string | null;
}

// @apache-airflow/ts-sdk/schedules: every function returns a Timetable
type RunImmediately = boolean | number;  // Python's None (10% of the interval) is not exposed
function cron(expression: string, timezone: string, options?: { interval?: number; runImmediately?: RunImmediately }): Timetable;
function cronDataInterval(expression: string, timezone: string): Timetable;
function multipleCron(expressions: string[], timezone: string, options?: { interval?: number; runImmediately?: RunImmediately }): Timetable;
function cronPartition(expression: string, timezone: string, options?: { runOffset?: number; runImmediately?: RunImmediately; keyFormat?: string }): Timetable;
function delta(delta: number, options?: { interval?: number }): Timetable;
function deltaDataInterval(delta: number): Timetable;
function events(dates: (Date | string)[], options?: { restrictToEvents?: boolean; presorted?: boolean; description?: string }): Timetable;
function once(): Timetable;
function continuous(): Timetable;
function none(): Timetable;
function partitionedAtRuntime(): Timetable;
// batchAssetEvents: unset takes Python's default; false is rejected for AND conditions and rollups
function asset(condition: AssetExpression, options?: { batchAssetEvents?: boolean }): Timetable;
function assetOrTime(condition: AssetExpression, timetable: Timetable, options?: { batchAssetEvents?: boolean }): Timetable;
function assetAndTime(condition: AssetExpression, timetable: Timetable): Timetable;
function partitionedAsset(condition: AssetExpression, options?: {
  defaultMapper?: PartitionMapper;      // default identity()
  mappers?: [AssetLike, PartitionMapper][];
  batchAssetEvents?: boolean;
}): Timetable;

// @apache-airflow/ts-sdk/mappers: every mapper takes maxDownstreamKeys in its options
type MapperOptions = { maxDownstreamKeys?: number };
type TemporalOptions = MapperOptions & { timezone?: string; inputFormat?: string; outputFormat?: string };
type Direction = "forward" | "backward";
function identity(options?: MapperOptions): PartitionMapper;
function fixedKey(downstreamKey: string, options?: MapperOptions): PartitionMapper;
function allowedKey(allowedKeys: string[], options?: MapperOptions): PartitionMapper;
function startOfHour(options?: TemporalOptions): PartitionMapper;
function startOfDay(options?: TemporalOptions): PartitionMapper;
function startOfWeek(options?: TemporalOptions): PartitionMapper;
function startOfMonth(options?: TemporalOptions): PartitionMapper;
function startOfQuarter(options?: TemporalOptions): PartitionMapper;
function startOfYear(options?: TemporalOptions): PartitionMapper;
function chain(mappers: PartitionMapper[], options?: MapperOptions): PartitionMapper;   // two or more
function product(mappers: PartitionMapper[], options?: MapperOptions & { delimiter?: string }): PartitionMapper;  // two or more
// A window matches the upstream mapper's granularity, such as startOfDay() with dayWindow().
function rollup(options: MapperOptions & { upstream: PartitionMapper; window: Window; waitPolicy?: WaitPolicy }): PartitionMapper;
// downstream is required for hourWindow and segmentWindow, which have no default
function fanOut(options: MapperOptions & { upstream: PartitionMapper; window: Window; downstream?: PartitionMapper }): PartitionMapper;
function hourWindow(options?: { direction?: Direction }): Window;
function dayWindow(options?: { direction?: Direction }): Window;
function weekWindow(options?: { direction?: Direction }): Window;
function monthWindow(options?: { direction?: Direction }): Window;
function quarterWindow(options?: { direction?: Direction }): Window;
function yearWindow(options?: { direction?: Direction }): Window;
function segmentWindow(segments: string[]): Window;
function waitForAll(): WaitPolicy;
function minimumCount(n: number): WaitPolicy;   // n != 0; negative counts from the expected total

// @apache-airflow/ts-sdk/python
function timetableRef(path: string, data?: JsonObject): Timetable;
function partitionMapperRef(path: string, data?: JsonObject): PartitionMapper;
function triggerRef(path: string, kwargs?: JsonObject, options?: { queue?: string }): TriggerRef;
```

## Example

The calls under "Builders" are standalone to show every option; a real Dag passes one timetable to
`schedule`.

```ts
import { Asset, AssetAlias, AssetWatcher, Dag, getClient, getContext } from "@apache-airflow/ts-sdk";
import * as mappers from "@apache-airflow/ts-sdk/mappers";
import { partitionMapperRef, timetableRef, triggerRef } from "@apache-airflow/ts-sdk/python";
import * as schedules from "@apache-airflow/ts-sdk/schedules";

// Definitions
const orders = new Asset({
  name: "orders",
  uri: "s3://lake/orders",
  group: "sales",
  extra: { owner: "sales" },
  accessControl: { producerTeams: ["sales"], consumerTeams: ["finance"], allowGlobal: false },
  watchers: [
    new AssetWatcher({
      name: "orders_queue",
      trigger: triggerRef("my_plugin.triggers.OrdersQueueTrigger", { queue_name: "orders" }, { queue: "default" }),
    }),
  ],
});
const customers = Asset.ref({ name: "customers" });
const rawFiles = Asset.ref({ uri: "s3://lake/raw" });
const exportsAlias = new AssetAlias({ name: "daily_exports", group: "sales" });

const both = orders.and(customers);
const either = Asset.any(orders, Asset.all(customers, rawFiles)).or(exportsAlias);

// Builders
schedules.cron("0 3 * * *", "Europe/Amsterdam", { interval: 3600, runImmediately: 7200 });
schedules.cronDataInterval("@daily", "UTC");
schedules.multipleCron(["0 3 * * 1-5", "0 9 * * 6"], "UTC", { interval: 600, runImmediately: true });
schedules.cronPartition("0 0 * * *", "UTC", { runOffset: -1, runImmediately: false, keyFormat: "%Y-%m-%d" });
schedules.delta(6 * 3600, { interval: 3600 });
schedules.deltaDataInterval(86400);
schedules.events([new Date("2026-12-24T00:00:00Z"), "2026-12-31T00:00:00+01:00"],
  { restrictToEvents: true, presorted: true, description: "Holidays" });
schedules.once();
schedules.continuous();
schedules.none();
schedules.partitionedAtRuntime();
schedules.asset(either, { batchAssetEvents: true });
schedules.assetOrTime(orders, schedules.cron("0 3 * * *", "UTC"), { batchAssetEvents: false });
schedules.assetAndTime(both, schedules.cron("0 3 * * *", "UTC"));
schedules.partitionedAsset(both, {
  defaultMapper: mappers.startOfDay({ timezone: "Europe/Amsterdam", inputFormat: "%Y-%m-%d", outputFormat: "%Y-%m-%d" }),
  mappers: [
    [customers, mappers.chain([mappers.startOfHour(), mappers.fixedKey("all")], { maxDownstreamKeys: 10 })],
    [orders, mappers.product([mappers.identity(), mappers.allowedKey(["eu", "us"])], { delimiter: "|", maxDownstreamKeys: 4 })],
  ],
  batchAssetEvents: true,
});
mappers.chain([mappers.startOfWeek(), mappers.startOfMonth({ maxDownstreamKeys: 1 })]);
mappers.rollup({ upstream: mappers.startOfHour(), window: mappers.hourWindow() });
mappers.rollup({ upstream: mappers.startOfDay(), window: mappers.dayWindow({ direction: "forward" }) });
mappers.rollup({ upstream: mappers.startOfMonth({ inputFormat: "%Y-%m-%d" }), window: mappers.monthWindow(), waitPolicy: mappers.minimumCount(-2) });
mappers.rollup({ upstream: mappers.fixedKey("all_regions"), window: mappers.segmentWindow(["eu", "us"]), waitPolicy: mappers.waitForAll() });
mappers.fanOut({ upstream: mappers.startOfWeek(), window: mappers.weekWindow({ direction: "backward" }), maxDownstreamKeys: 7 });
mappers.fanOut({ upstream: mappers.startOfQuarter(), window: mappers.quarterWindow() });
mappers.fanOut({ upstream: mappers.startOfYear(), window: mappers.yearWindow() });
mappers.fanOut({ upstream: mappers.identity(), window: mappers.segmentWindow(["eu", "us"]), downstream: mappers.identity() });
partitionMapperRef("my_plugin.mappers.FiscalQuarter", { start_month: 4 });

// Dags and a task
new Dag("cron_report", { schedule: "0 3 * * *", queue: "typescript" });
new Dag("workday_report", { schedule: timetableRef("my_plugin.timetables.Workday", { holidays: "us" }), queue: "typescript" });
const dag = new Dag("daily_report", { schedule: schedules.asset(both), queue: "typescript" });

dag.task("load", async () => {
  const ctx = getContext();
  const client = getClient();

  const why = ctx.triggeringAssetEvents(orders);

  const recent = await client.inletEvents(orders).query({
    after: new Date("2026-11-01T00:00:00Z"),
    before: "2026-11-23T00:00:00+01:00",
    ascending: false,
    limit: 10,
    partitionKey: "2026-11-22",
    partitionKeyPattern: "^2026-11-",
    extra: { region: "eu" },
  });
  const oldest = await client.inletEvents(customers).first();
  const latest = await client.inletEvents(exportsAlias).last({ partitionKey: "2026-11-22" });

  ctx.outletEvents(orders).setExtra({ rows: recent.length, sources: why.flatMap((e) => e.sourceAliases) });
  ctx.outletEvents(orders).addPartitions(["2026-11-22"]);
  ctx.outletEvents(exportsAlias).add(new Asset({ name: "export_2026_11_22", uri: "s3://lake/exports/2026-11-22" }), { rows: 10 });

  const store = client.assetStateStore(orders);
  const mark = latest ?? oldest;
  if (mark) {
    await store.set("watermark", mark.timestamp.toISOString());
  }
  const watermark = await store.get<string>("watermark");
  await store.delete("watermark");
  await store.clear();
  return watermark;
}, { inlets: [orders, customers, exportsAlias], outlets: [orders, exportsAlias] });
```

## Consequences

- Widening `DagSpec.schedule` keeps every existing Dag compiling, since a string is still accepted.
- `add()` on a non-alias accessor throws, where Python ignores it.
- The builders must change whenever a Python built-in's `serialize()` changes; the golden fixtures
  from ADR-0019 catch that in CI.
