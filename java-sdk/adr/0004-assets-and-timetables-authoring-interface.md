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
This ADR records the Java spelling of it on the interface surface (`DagDef`) and on the annotation
surface of [ADR-0002](0002-native-dag-interface.md).

## Decision

1. **`Asset` and `AssetAlias` use constructor overloads.** A URI-only asset uses
   `Asset.fromUri(uri)`, and refs use `AssetRef.byName(name)` and `AssetRef.byUri(uri)`, because a
   name and a URI are both a `String` and an overload cannot tell them apart. `withAccessControl`
   and `withWatchers` return a new, immutable `Asset`. Java has no type alias, so asset and event
   `extra` stay `Map<String, Object>`.
2. **Conditions use `Asset.all(...)`, `Asset.any(...)`, `.and(...)` and `.or(...)`.**
3. **Builders live in classes used as namespaces**: `org.apache.airflow.sdk.schedule.Schedules`,
   `org.apache.airflow.sdk.schedule.Mappers` and `org.apache.airflow.sdk.python.Python`.
   `Timetable`, `PartitionMapper`, `Window`, `WaitPolicy` and `Trigger` are opaque interfaces.
4. **One optional argument is an overload; two or more are a `Consumer` of an options object**,
   such as `cron(expression, timezone, o -> o.interval(...))`. The SDK creates the options object,
   so callers never write `new`, `builder()` or `build()`. A Python argument with no default, such
   as a cron timezone, is a required positional argument.
5. **`DagDef` gets `schedule(String)` and `schedule(Timetable)` overloads.** `config("schedule", ...)`
   accepts the same two types, and the generated schema field, the config value check and the
   serializer are widened to match.
6. **On the annotation surface, `@Airflow.Schedule` marks a static, no-argument method that returns
   a `Timetable`**, since annotation attributes take only constants. The `schedule` attribute of
   `@Airflow.Dag` keeps taking a cron expression or preset. The processor rejects a class that sets
   both, or a method that is not static, takes arguments or returns another type.
7. **`TaskRef` gets `inlets(AssetLike...)` and `outlets(AssetLike...)`.** On the annotation surface
   the wiring view returns the same `TaskRef`, so they apply inside `depends()`.
8. **Outlet and triggering events are on `Context`; inlet events and the asset state store are on
   `Client`**, the two arguments `Task.execute` already receives. The asset state store mirrors
   `TaskStateStore` without retention, which the asset messages do not carry. Lookups that may find
   nothing return `null`, as the existing `Client` methods do.
9. **Inlet events take a `Consumer<InletEventQueryParams>`**, applied once when `query`, `first` or
   `last` is called. `extra(key, value)` is repeatable, as in Python.
10. **Durations are `java.time.Duration`; datetimes are `OffsetDateTime` or `Instant`.** A
    `runImmediately` duration is sent as a `Double`, as Python writes it.
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
  Asset(String name, String uri, Consumer<AssetOptions> options);
  static Asset fromUri(String uri);
  Asset withAccessControl(Consumer<AssetAccessControl> accessControl);
  Asset withWatchers(AssetWatcher... watchers);
  static AssetExpression all(AssetExpression... items);
  static AssetExpression any(AssetExpression... items);
}
final class AssetOptions {                                  // group(String), default "asset";
}                                                           // extra(Map<String, Object>)
final class AssetAccessControl {                            // unset fields take Python's defaults
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
DagDef schedule(String expression);                         // cron expression or preset
DagDef schedule(Timetable timetable);
DagDef config(String key, Object value);                    // "schedule": String or Timetable
TaskRef<T> inlets(AssetLike... assets);
TaskRef<T> outlets(AssetLike... assets);

// Context
OutletEventAccessor outletEvents(AssetLike asset);          // IllegalStateException if asset is not an
                                                            //   outlet, or in a stub TaskHandler
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
  List<AssetEvent> query(Consumer<InletEventQueryParams> params);
  AssetEvent first();                                       // oldest, or null when none; the params of
  AssetEvent first(Consumer<InletEventQueryParams> params); //   first and last leave ascending and limit unset
  AssetEvent last();                                        // newest, or null when none
  AssetEvent last(Consumer<InletEventQueryParams> params);
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

// org.apache.airflow.sdk.schedule options; callers receive them in a Consumer
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
static Timetable cron(String expression, String timezone, Consumer<CronOptions> options);
static Timetable cronDataInterval(String expression, String timezone);
static Timetable multipleCron(List<String> expressions, String timezone);
static Timetable multipleCron(List<String> expressions, String timezone, Consumer<CronOptions> options);
static Timetable cronPartition(String expression, String timezone);
static Timetable cronPartition(String expression, String timezone, Consumer<CronPartitionOptions> options);
static Timetable delta(Duration delta);
static Timetable delta(Duration delta, Duration interval);
static Timetable deltaDataInterval(Duration delta);
static Timetable events(List<OffsetDateTime> dates);
static Timetable events(List<OffsetDateTime> dates, Consumer<EventsOptions> options);
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
static Timetable partitionedAsset(AssetExpression condition, Consumer<PartitionedAssetOptions> options);

// org.apache.airflow.sdk.schedule.Mappers: every mapper takes maxDownstreamKeys
static PartitionMapper identity();
static PartitionMapper identity(int maxDownstreamKeys);
static PartitionMapper fixedKey(String downstreamKey);
static PartitionMapper fixedKey(String downstreamKey, int maxDownstreamKeys);
static PartitionMapper allowedKey(List<String> allowedKeys);
static PartitionMapper allowedKey(List<String> allowedKeys, int maxDownstreamKeys);
static PartitionMapper startOfHour();       // startOfDay, startOfWeek, startOfMonth, startOfQuarter and
static PartitionMapper startOfHour(Consumer<TemporalOptions> options);  // startOfYear have the same overloads
static PartitionMapper chain(List<PartitionMapper> mappers);    // two or more
static PartitionMapper chain(List<PartitionMapper> mappers, int maxDownstreamKeys);
static PartitionMapper product(List<PartitionMapper> mappers);  // two or more
static PartitionMapper product(List<PartitionMapper> mappers, Consumer<ProductOptions> options);
// A window matches the upstream mapper's granularity, such as startOfDay() with dayWindow().
static PartitionMapper rollup(PartitionMapper upstream, Window window);
static PartitionMapper rollup(PartitionMapper upstream, Window window, Consumer<RollupOptions> options);
static PartitionMapper fanOut(PartitionMapper upstream, Window window);  // hour and segment windows need
                                                                         // FanOutOptions.downstream
static PartitionMapper fanOut(PartitionMapper upstream, Window window, Consumer<FanOutOptions> options);
static Window hourWindow();                 // dayWindow, weekWindow, monthWindow, quarterWindow and
static Window hourWindow(Direction direction);  // yearWindow have the same two overloads
static Window segmentWindow(List<String> segments);
static WaitPolicy waitForAll();
static WaitPolicy minimumCount(int n);      // n != 0; negative counts from the expected total
enum Direction { FORWARD, BACKWARD }

// org.apache.airflow.sdk.Airflow (annotation surface)
@interface Schedule {}                                      // on a static Timetable method with no arguments

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
  static final Asset ORDERS = new Asset("orders", "s3://lake/orders", o -> o.group("sales").extra(Map.of("owner", "sales")))
      .withAccessControl(a -> a.producerTeams(List.of("sales")).consumerTeams(List.of("finance")).allowGlobal(false))
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
      Schedules.cron("0 3 * * *", "Europe/Amsterdam", o -> o.interval(Duration.ofHours(1))
          .runImmediately(Duration.ofHours(2))),
      Schedules.cronDataInterval("@daily", "UTC"),
      Schedules.multipleCron(List.of("0 3 * * 1-5", "0 9 * * 6"), "UTC"),
      Schedules.multipleCron(List.of("0 3 * * 1-5"), "UTC", o -> o.runImmediately(true)),
      Schedules.cronPartition("0 0 * * *", "UTC"),
      Schedules.cronPartition("0 0 * * *", "UTC", o -> o.runImmediately(Duration.ofHours(1))),
      Schedules.cronPartition("0 0 * * *", "UTC", o -> o.runOffset(-1).runImmediately(false)
          .keyFormat("%Y-%m-%d")),
      Schedules.delta(Duration.ofHours(6)),
      Schedules.delta(Duration.ofHours(6), Duration.ofHours(1)),
      Schedules.deltaDataInterval(Duration.ofDays(1)),
      Schedules.events(List.of(OffsetDateTime.parse("2026-12-24T00:00:00Z"))),
      Schedules.events(List.of(OffsetDateTime.parse("2026-12-31T00:00:00+01:00")), o -> o
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
      Schedules.partitionedAsset(BOTH, o -> o
          .defaultMapper(startOfDay(t -> t.timezone("Europe/Amsterdam").inputFormat("%Y-%m-%d")
              .outputFormat("%Y-%m-%d").maxDownstreamKeys(1)))
          .mapper(CUSTOMERS, chain(List.of(startOfHour(), fixedKey("all")), 10))
          .mapper(ORDERS, product(List.of(identity(), allowedKey(List.of("eu", "us"))),
              p -> p.delimiter("|").maxDownstreamKeys(4)))
          .batchAssetEvents(true)),
      Python.timetableRef("my_plugin.timetables.Workday", Map.of("holidays", "us")));

  static final List<PartitionMapper> MAPPERS = List.of(
      identity(2),
      fixedKey("all", 1),
      allowedKey(List.of("eu"), 1),
      chain(List.of(startOfWeek(), startOfMonth())),
      product(List.of(startOfQuarter(), startOfYear())),
      rollup(startOfHour(t -> t.timezone("UTC")), hourWindow()),
      rollup(startOfDay(), dayWindow(Direction.FORWARD)),
      rollup(startOfMonth(t -> t.inputFormat("%Y-%m-%d")), monthWindow(),
          r -> r.waitPolicy(minimumCount(-2)).maxDownstreamKeys(1)),
      rollup(fixedKey("all_regions"), segmentWindow(List.of("eu", "us")), r -> r.waitPolicy(waitForAll())),
      rollup(startOfMonth(), monthWindow(Direction.BACKWARD)),
      fanOut(startOfWeek(t -> t.outputFormat("%Y-%m-%d")), weekWindow()),
      fanOut(startOfWeek(), weekWindow(Direction.BACKWARD), f -> f.maxDownstreamKeys(7)),
      fanOut(startOfHour(), hourWindow(Direction.FORWARD), f -> f.downstream(startOfHour())),
      fanOut(identity(), segmentWindow(List.of("eu", "us")), f -> f.downstream(identity())),
      fanOut(startOfQuarter(t -> t.timezone("UTC")), quarterWindow()),
      fanOut(startOfQuarter(), quarterWindow(Direction.FORWARD)),
      fanOut(startOfYear(t -> t.timezone("UTC")), yearWindow()),
      fanOut(startOfYear(), yearWindow(Direction.BACKWARD)),
      rollup(startOfDay(t -> t.timezone("UTC")), dayWindow()),
      Python.partitionMapperRef("my_plugin.mappers.FiscalQuarter", Map.of("start_month", 4)));

  public static List<DagDef> dags() {
    var cron = new DagDef("cron_report").schedule("0 3 * * *").config("queue", "java");
    cron.task("load", Load.class);
    var legacy = new DagDef("legacy_report").config("schedule", "@daily");
    legacy.task("load", Load.class);
    var daily = new DagDef("daily_report").schedule(Schedules.asset(BOTH)).config("queue", "java");
    daily.task("load", Load.class)
        .inlets(ORDERS, CUSTOMERS, EXPORTS, ARCHIVE)
        .outlets(ORDERS, EXPORTS, ADHOC, EVENTS);
    return List.of(cron, legacy, daily);
  }

  public static class Load implements Task {
    @Override
    public void execute(Context context, Client client) {
      List<TriggeringAssetEvent> why = context.triggeringAssetEvents(ORDERS);

      List<AssetEvent> recent = client.inletEvents(ORDERS).query(p -> p
          .after(OffsetDateTime.parse("2026-11-01T00:00:00Z"))
          .before(Instant.parse("2026-11-23T00:00:00Z"))
          .ascending(false)
          .limit(10)
          .partitionKey("2026-11-22")
          .partitionKeyPattern("^2026-11-")
          .extra("region", "eu")
          .extra("tier", "gold"));
      List<AssetEvent> all = client.inletEvents(ARCHIVE).query(p -> p
          .after(Instant.parse("2026-01-01T00:00:00Z"))
          .before(OffsetDateTime.parse("2026-12-31T00:00:00Z")));
      List<AssetEvent> raw = client.inletEvents(RAW_FILES).query();
      AssetEvent oldest = client.inletEvents(CUSTOMERS).first();
      AssetEvent oldestToday = client.inletEvents(CUSTOMERS).first(p -> p.partitionKey("2026-11-22"));
      AssetEvent latest = client.inletEvents(EXPORTS).last();
      AssetEvent latestEu = client.inletEvents(EXPORTS).last(p -> p.extra("region", "eu"));

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

The annotation surface spells a timetable schedule with `@Airflow.Schedule`:

```java
@Airflow.Dag(id = "daily_report_annotated", queue = "java")
public class DailyReport {
  static final Asset ORDERS = new Asset("orders", "s3://lake/orders");

  @Airflow.Schedule
  static Timetable schedule() {
    return Schedules.asset(ORDERS);
  }

  @Airflow.Task
  public void load(Context context) {
    context.outletEvents(ORDERS).setExtra(Map.of("rows", 1));
  }

  @Airflow.Deps
  static class Wiring implements DailyReportDeps {
    void depends() {
      load().inlets(ORDERS).outlets(ORDERS);
    }
  }
}

@Airflow.Dag(id = "cron_report_annotated", schedule = "0 3 * * *")   // unchanged
public class CronReport { /* ... */ }
```

## Consequences

- Existing `config("schedule", "...")` calls and `@Airflow.Dag(schedule = "...")` attributes keep
  working.
- `outletEvents` for an asset outside the task's outlets or in a stub TaskHandler, and `add` on a
  non-alias accessor, throw where Python drops the event silently.
- The builders must change whenever a Python built-in's `serialize()` changes; the golden fixtures
  from core ADR-0019 catch that in CI.
