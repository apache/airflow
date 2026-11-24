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

# ADR-0004: Java Authoring Interface for Assets and Timetables

## Status

Proposed.

## Context

The shared contract for assets, timetables and Python references is in
[`airflow-core/adr/lang-sdk/0019`](../../airflow-core/adr/lang-sdk/0019-assets-and-timetables-across-lang-sdks.md).
This ADR records the Java spelling of it on the interface surface (`DagDef`). On the annotation
surface of [ADR-0002](0002-native-dag-interface.md), the generated wiring view returns the same
`TaskRef`, so `inlets` and `outlets` apply inside `depends()`. A timetable schedule there is left
to a follow-up, since annotation attributes take only constants.

## Decision

1. **`Asset` and `AssetAlias` use constructor overloads.** A URI-only asset uses
   `Asset.fromUri(uri)`, and refs use `AssetRef.byName(name)` and `AssetRef.byUri(uri)`, because a
   name and a URI are both a `String` and an overload cannot tell them apart. `withAccessControl`
   and `withWatchers` return a new, immutable `Asset`.
2. **Conditions use `Asset.all(...)`, `Asset.any(...)`, `.and(...)` and `.or(...)`.**
3. **Builders live in classes used as namespaces**: `org.apache.airflow.sdk.schedule.Schedules`,
   `org.apache.airflow.sdk.schedule.Mappers` and `org.apache.airflow.sdk.python.Python`.
   `Timetable`, `PartitionMapper`, `Window`, `WaitPolicy` and `Trigger` are opaque interfaces.
4. **One optional argument is an overload; two or more are an options object** with a no-arg
   constructor and fluent setters, such as `new CronOptions().interval(...)`. There is no
   `builder()` or `build()`. A Python argument with no default, such as a cron timezone, is a
   required positional argument.
5. **`schedule` is widened to `String` or `Timetable`** in the generated schema field, the config
   value check and the serializer, which accept only a `String` today.
6. **`TaskRef` gets `inlets(AssetLike...)` and `outlets(AssetLike...)`.**
7. **Outlet and triggering events are on `Context`; inlet events and the asset state store are on
   `Client`**, the two arguments `Task.execute` already receives. The asset state store mirrors
   `TaskStateStore` without retention, which the asset messages do not carry. Lookups that may find
   nothing return `null`, as the existing `Client` methods do.
8. **Inlet events take `InletEventQueryParams`**, a mutable params object read once when `query`,
   `first` or `last` is called. `extra(key, value)` is repeatable, as in Python.
9. **Durations are `java.time.Duration`; datetimes are `OffsetDateTime` or `Instant`.**
   `LocalDateTime` is not accepted, since it has no offset.

## Signatures

```java
// org.apache.airflow.sdk
interface AssetExpression {
  AssetExpression and(AssetExpression... others);
  AssetExpression or(AssetExpression... others);
}
interface AssetLike extends AssetExpression {}              // Asset, AssetRef, AssetAlias

final class Asset implements AssetLike {
  Asset(String name);                                       // the URI defaults to the name
  Asset(String name, String uri);
  Asset(String name, String uri, AssetOptions options);
  static Asset fromUri(String uri);
  Asset withAccessControl(AssetAccessControl accessControl);
  Asset withWatchers(AssetWatcher... watchers);
  static AssetExpression all(AssetExpression... items);
  static AssetExpression any(AssetExpression... items);
}
final class AssetOptions {                                  // group(String), default "asset";
}                                                           // extra(Map<String, Object>)
final class AssetAccessControl {                            // unset fields take Python's defaults
  AssetAccessControl();
  AssetAccessControl producerTeams(List<String> teams);     // default empty
  AssetAccessControl consumerTeams(List<String> teams);     // default unset
  AssetAccessControl allowGlobal(boolean allowGlobal);      // default true
}
final class AssetRef implements AssetLike {
  static AssetRef byName(String name);
  static AssetRef byUri(String uri);
}
final class AssetAlias implements AssetLike {
  AssetAlias(String name);
  AssetAlias(String name, String group);                    // group defaults to "asset"
}
final class AssetWatcher {
  AssetWatcher(String name, Trigger trigger);               // trigger must name a BaseEventTrigger
}
interface Timetable {}                                      // these opaque types are in this package;
interface PartitionMapper {}                                // Window, WaitPolicy and Direction are
interface Trigger {}                                        // in org.apache.airflow.sdk.schedule

// DagDef and TaskRef
DagDef config(String key, Object value);                    // "schedule": String or Timetable
TaskRef<T> inlets(AssetLike... assets);
TaskRef<T> outlets(AssetLike... assets);

// Context
OutletEventAccessor outletEvents(AssetLike asset);
List<TriggeringAssetEvent> triggeringAssetEvents(AssetLike asset);
interface OutletEventAccessor {
  void setExtra(Map<String, Object> extra);
  void addPartitions(String... keys);                       // IllegalStateException on an alias
  void add(Asset asset);                                    // IllegalStateException on a non-alias
  void add(Asset asset, Map<String, Object> extra);
  void add(AssetRef asset);
  void add(AssetRef asset, Map<String, Object> extra);
}

// Client
InletEventAccessor inletEvents(AssetLike asset);
AssetStateStore assetStateStore(Asset asset);
AssetStateStore assetStateStore(AssetRef asset);
final class InletEventQueryParams {
  InletEventQueryParams();
  InletEventQueryParams after(OffsetDateTime after);
  InletEventQueryParams after(Instant after);
  InletEventQueryParams before(OffsetDateTime before);
  InletEventQueryParams before(Instant before);
  InletEventQueryParams ascending(boolean ascending);       // default true
  InletEventQueryParams limit(int limit);
  InletEventQueryParams partitionKey(String key);
  InletEventQueryParams partitionKeyPattern(String pattern);
  InletEventQueryParams extra(String key, String value);
}
interface InletEventAccessor {
  List<AssetEvent> query();
  List<AssetEvent> query(InletEventQueryParams params);
  AssetEvent first();                                       // oldest, or null when none; the params of
  AssetEvent first(InletEventQueryParams params);           //   first and last leave ascending and limit unset
  AssetEvent last();                                        // newest, or null when none
  AssetEvent last(InletEventQueryParams params);
}
interface AssetStateStore {
  Object get(String key);                                   // null when absent
  void set(String key, Object value);
  void delete(String key);
  void clear();
}
// AssetEvent getters: getId, getTimestamp (OffsetDateTime), getExtra, getAsset (name, uri, group,
// extra), getSourceDagId, getSourceTaskId, getSourceRunId, getSourceMapIndex (Integer),
// getPartitionKey. created_dagruns is not exposed. TriggeringAssetEvent, from consumed_asset_events,
// has the same getters except getId, has no group in getAsset, and adds getSourceAliases (List<String>).

// org.apache.airflow.sdk.schedule options
final class CronOptions {            // interval(Duration), runImmediately(boolean), runImmediately(Duration);
}                                    // the last call wins. run_immediately=None is not exposed.
final class CronPartitionOptions {   // runOffset(int), runImmediately(boolean | Duration), keyFormat(String)
}
final class EventsOptions {          // restrictToEvents(boolean), presorted(boolean), description(String)
}
final class PartitionedAssetOptions {  // defaultMapper(PartitionMapper), mapper(AssetLike, PartitionMapper),
}                                      // batchAssetEvents(boolean)
final class TemporalOptions {        // timezone(String), inputFormat(String), outputFormat(String),
}                                    // maxDownstreamKeys(int)
final class ProductOptions {         // delimiter(String), maxDownstreamKeys(int)
}
final class RollupOptions {          // waitPolicy(WaitPolicy), maxDownstreamKeys(int)
}
final class FanOutOptions {          // downstream(PartitionMapper), maxDownstreamKeys(int)
}

// org.apache.airflow.sdk.schedule.Schedules: every method returns Timetable.
// batchAssetEvents unset takes Python's default; false is rejected for AND conditions and rollups.
static Timetable cron(String expression, String timezone);
static Timetable cron(String expression, String timezone, CronOptions options);
static Timetable cronDataInterval(String expression, String timezone);
static Timetable multipleCron(List<String> expressions, String timezone);
static Timetable multipleCron(List<String> expressions, String timezone, CronOptions options);
static Timetable cronPartition(String expression, String timezone);
static Timetable cronPartition(String expression, String timezone, CronPartitionOptions options);
static Timetable delta(Duration delta);
static Timetable delta(Duration delta, Duration interval);
static Timetable deltaDataInterval(Duration delta);
static Timetable events(List<OffsetDateTime> dates);
static Timetable events(List<OffsetDateTime> dates, EventsOptions options);
static Timetable once();
static Timetable continuous();
static Timetable none();
static Timetable partitionedAtRuntime();
static Timetable asset(AssetExpression condition);
static Timetable asset(AssetExpression condition, boolean batchAssetEvents);
static Timetable assetOrTime(AssetExpression condition, Timetable timetable);
static Timetable assetOrTime(AssetExpression condition, Timetable timetable, boolean batchAssetEvents);
static Timetable assetAndTime(AssetExpression condition, Timetable timetable);
static Timetable partitionedAsset(AssetExpression condition);   // identity() mapper
static Timetable partitionedAsset(AssetExpression condition, PartitionedAssetOptions options);

// org.apache.airflow.sdk.schedule.Mappers: every mapper takes maxDownstreamKeys
static PartitionMapper identity();
static PartitionMapper identity(int maxDownstreamKeys);
static PartitionMapper fixedKey(String downstreamKey);
static PartitionMapper fixedKey(String downstreamKey, int maxDownstreamKeys);
static PartitionMapper allowedKey(List<String> allowedKeys);
static PartitionMapper allowedKey(List<String> allowedKeys, int maxDownstreamKeys);
static PartitionMapper startOfHour();       // startOfDay, startOfWeek, startOfMonth, startOfQuarter and
static PartitionMapper startOfHour(TemporalOptions options);  // startOfYear have the same two overloads
static PartitionMapper chain(List<PartitionMapper> mappers);    // two or more
static PartitionMapper chain(List<PartitionMapper> mappers, int maxDownstreamKeys);
static PartitionMapper product(List<PartitionMapper> mappers);  // two or more
static PartitionMapper product(List<PartitionMapper> mappers, ProductOptions options);
// A window matches the upstream mapper's granularity, such as startOfDay() with dayWindow().
static PartitionMapper rollup(PartitionMapper upstream, Window window);
static PartitionMapper rollup(PartitionMapper upstream, Window window, RollupOptions options);
static PartitionMapper fanOut(PartitionMapper upstream, Window window);  // hour and segment windows need
                                                                         // FanOutOptions.downstream
static PartitionMapper fanOut(PartitionMapper upstream, Window window, FanOutOptions options);
static Window hourWindow();                 // dayWindow, weekWindow, monthWindow, quarterWindow and
static Window hourWindow(Direction direction);  // yearWindow have the same two overloads
static Window segmentWindow(List<String> segments);
static WaitPolicy waitForAll();
static WaitPolicy minimumCount(int n);      // n != 0; negative counts from the expected total
enum Direction { FORWARD, BACKWARD }

// org.apache.airflow.sdk.python.Python
static Timetable timetableRef(String path, Map<String, Object> data);
static PartitionMapper partitionMapperRef(String path, Map<String, Object> data);
static Trigger triggerRef(String path, Map<String, Object> kwargs);
static Trigger triggerRef(String path, Map<String, Object> kwargs, String queue);
```

## Example

The builder lists show every overload and option; a real Dag passes one timetable to `schedule`.

```java
import static org.apache.airflow.sdk.schedule.Mappers.*;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.apache.airflow.sdk.*;
import org.apache.airflow.sdk.python.Python;
import org.apache.airflow.sdk.schedule.*;

public class Reports {
  static final Asset ORDERS = new Asset("orders", "s3://lake/orders", new AssetOptions().group("sales")
          .extra(Map.of("owner", "sales")))
      .withAccessControl(new AssetAccessControl().producerTeams(List.of("sales")).consumerTeams(List.of("finance"))
          .allowGlobal(false))
      .withWatchers(new AssetWatcher("orders_queue",
          Python.triggerRef("my_plugin.triggers.OrdersQueueTrigger", Map.of("queue_name", "orders"), "default")));
  static final Asset INVOICES = new Asset("invoices");
  static final Asset EVENTS = new Asset("events", "s3://lake/events");
  static final Asset ARCHIVE = Asset.fromUri("s3://lake/archive");
  static final AssetRef CUSTOMERS = AssetRef.byName("customers");
  static final AssetRef RAW_FILES = AssetRef.byUri("s3://lake/raw");
  static final AssetAlias EXPORTS = new AssetAlias("daily_exports", "sales");
  static final AssetAlias ADHOC = new AssetAlias("adhoc_exports");
  static final Trigger PLAIN = Python.triggerRef("my_plugin.triggers.FileTrigger", Map.of("path", "/data"));

  static final AssetExpression BOTH = ORDERS.and(CUSTOMERS);
  static final AssetExpression EITHER = Asset.any(ORDERS, Asset.all(CUSTOMERS, RAW_FILES)).or(INVOICES);

  static final List<Timetable> TIMETABLES = List.of(
      Schedules.cron("0 3 * * *", "UTC"),
      Schedules.cron("0 3 * * *", "Europe/Amsterdam", new CronOptions().interval(Duration.ofHours(1))
          .runImmediately(Duration.ofHours(2))),
      Schedules.cronDataInterval("@daily", "UTC"),
      Schedules.multipleCron(List.of("0 3 * * 1-5", "0 9 * * 6"), "UTC"),
      Schedules.multipleCron(List.of("0 3 * * 1-5"), "UTC", new CronOptions().runImmediately(true)),
      Schedules.cronPartition("0 0 * * *", "UTC"),
      Schedules.cronPartition("0 0 * * *", "UTC", new CronPartitionOptions().runImmediately(Duration.ofHours(1))),
      Schedules.cronPartition("0 0 * * *", "UTC", new CronPartitionOptions().runOffset(-1).runImmediately(false)
          .keyFormat("%Y-%m-%d")),
      Schedules.delta(Duration.ofHours(6)),
      Schedules.delta(Duration.ofHours(6), Duration.ofHours(1)),
      Schedules.deltaDataInterval(Duration.ofDays(1)),
      Schedules.events(List.of(OffsetDateTime.parse("2026-12-24T00:00:00Z"))),
      Schedules.events(List.of(OffsetDateTime.parse("2026-12-31T00:00:00+01:00")), new EventsOptions()
          .restrictToEvents(true).presorted(true).description("Holidays")),
      Schedules.once(),
      Schedules.continuous(),
      Schedules.none(),
      Schedules.partitionedAtRuntime(),
      Schedules.asset(EITHER),
      Schedules.asset(EITHER, true),
      Schedules.assetOrTime(ORDERS, Schedules.cron("0 3 * * *", "UTC")),
      Schedules.assetOrTime(ORDERS, Schedules.cron("0 3 * * *", "UTC"), false),
      Schedules.assetAndTime(BOTH, Schedules.cron("0 3 * * *", "UTC")),
      Schedules.partitionedAsset(ORDERS),
      Schedules.partitionedAsset(BOTH, new PartitionedAssetOptions()
          .defaultMapper(startOfDay(new TemporalOptions().timezone("Europe/Amsterdam").inputFormat("%Y-%m-%d")
              .outputFormat("%Y-%m-%d").maxDownstreamKeys(1)))
          .mapper(CUSTOMERS, chain(List.of(startOfHour(), fixedKey("all")), 10))
          .mapper(ORDERS, product(List.of(identity(), allowedKey(List.of("eu", "us"))),
              new ProductOptions().delimiter("|").maxDownstreamKeys(4)))
          .batchAssetEvents(true)),
      Python.timetableRef("my_plugin.timetables.Workday", Map.of("holidays", "us")));

  static final List<PartitionMapper> MAPPERS = List.of(
      identity(2),
      fixedKey("all", 1),
      allowedKey(List.of("eu"), 1),
      chain(List.of(startOfWeek(), startOfMonth())),
      product(List.of(startOfQuarter(), startOfYear())),
      rollup(startOfHour(new TemporalOptions().timezone("UTC")), hourWindow()),
      rollup(startOfDay(), dayWindow(Direction.FORWARD)),
      rollup(startOfMonth(new TemporalOptions().inputFormat("%Y-%m-%d")), monthWindow(),
          new RollupOptions().waitPolicy(minimumCount(-2)).maxDownstreamKeys(1)),
      rollup(fixedKey("all_regions"), segmentWindow(List.of("eu", "us")), new RollupOptions().waitPolicy(waitForAll())),
      rollup(startOfMonth(), monthWindow(Direction.BACKWARD)),
      fanOut(startOfWeek(new TemporalOptions().outputFormat("%Y-%m-%d")), weekWindow()),
      fanOut(startOfWeek(), weekWindow(Direction.BACKWARD), new FanOutOptions().maxDownstreamKeys(7)),
      fanOut(startOfHour(), hourWindow(Direction.FORWARD), new FanOutOptions().downstream(startOfHour())),
      fanOut(identity(), segmentWindow(List.of("eu", "us")), new FanOutOptions().downstream(identity())),
      fanOut(startOfQuarter(new TemporalOptions().timezone("UTC")), quarterWindow()),
      fanOut(startOfQuarter(), quarterWindow(Direction.FORWARD)),
      fanOut(startOfYear(new TemporalOptions().timezone("UTC")), yearWindow()),
      fanOut(startOfYear(), yearWindow(Direction.BACKWARD)),
      rollup(startOfDay(new TemporalOptions().timezone("UTC")), dayWindow()),
      Python.partitionMapperRef("my_plugin.mappers.FiscalQuarter", Map.of("start_month", 4)));

  public static List<DagDef> dags() {
    var cron = new DagDef("cron_report").config("schedule", "0 3 * * *").config("queue", "java");
    cron.task("load", Load.class);
    var daily = new DagDef("daily_report").config("schedule", Schedules.asset(BOTH)).config("queue", "java");
    daily.task("load", Load.class)
        .inlets(ORDERS, CUSTOMERS, EXPORTS, ARCHIVE)
        .outlets(ORDERS, EXPORTS, ADHOC, EVENTS);
    return List.of(cron, daily);
  }

  public static class Load implements Task {
    @Override
    public void execute(Context context, Client client) {
      List<TriggeringAssetEvent> why = context.triggeringAssetEvents(ORDERS);

      List<AssetEvent> recent = client.inletEvents(ORDERS).query(new InletEventQueryParams()
          .after(OffsetDateTime.parse("2026-11-01T00:00:00Z"))
          .before(Instant.parse("2026-11-23T00:00:00Z"))
          .ascending(false)
          .limit(10)
          .partitionKey("2026-11-22")
          .partitionKeyPattern("^2026-11-")
          .extra("region", "eu")
          .extra("tier", "gold"));
      List<AssetEvent> all = client.inletEvents(ARCHIVE).query(new InletEventQueryParams()
          .after(Instant.parse("2026-01-01T00:00:00Z"))
          .before(OffsetDateTime.parse("2026-12-31T00:00:00Z")));
      List<AssetEvent> raw = client.inletEvents(RAW_FILES).query();
      AssetEvent oldest = client.inletEvents(CUSTOMERS).first();
      AssetEvent oldestToday = client.inletEvents(CUSTOMERS).first(new InletEventQueryParams().partitionKey("2026-11-22"));
      AssetEvent latest = client.inletEvents(EXPORTS).last();
      AssetEvent latestEu = client.inletEvents(EXPORTS).last(new InletEventQueryParams().extra("region", "eu"));

      context.outletEvents(ORDERS).setExtra(Map.of("rows", recent.size(), "archived", all.size(), "raw", raw.size(),
          "triggers", why.size()));
      context.outletEvents(ORDERS).addPartitions("2026-11-22", "2026-11-23");
      context.outletEvents(EXPORTS).add(new Asset("export_2026_11_22", "s3://lake/exports/2026-11-22"), Map.of("rows", 10));
      context.outletEvents(ADHOC).add(CUSTOMERS);
      context.outletEvents(ADHOC).add(INVOICES);
      context.outletEvents(EXPORTS).add(RAW_FILES, Map.of("rows", 1));

      AssetStateStore store = client.assetStateStore(ORDERS);
      for (AssetEvent mark : new AssetEvent[] {latest, latestEu, oldestToday, oldest}) {
        if (mark != null) {
          store.set("watermark", mark.getTimestamp().toString());
          break;
        }
      }
      Object watermark = store.get("watermark");
      client.assetStateStore(CUSTOMERS).set("seen", watermark != null);
      store.delete("watermark");
      store.clear();
    }
  }
}
```

## Consequences

- Existing `config("schedule", "...")` calls keep working.
- `add` on a non-alias accessor throws, where Python ignores it.
- The builders must change whenever a Python built-in's `serialize()` changes; the golden fixtures
  from core ADR-0019 catch that in CI.
