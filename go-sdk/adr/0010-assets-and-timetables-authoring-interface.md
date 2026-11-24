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

# 10. Assets and timetables authoring interface

Date: 2026-11-24

## Status

Proposed.

## Context

The shared contract for assets, timetables and Python references is in
[`airflow-core/adr/lang-sdk/0019`](../../airflow-core/adr/lang-sdk/0019-assets-and-timetables-across-lang-sdks.md).
This ADR records the Go spelling of it.

## Decision

1. **Assets are structs**: `airflow.Asset`, `airflow.AssetAlias`, `airflow.AssetWatcher`, and
   `airflow.AssetRef` with either `Name` or `URI` set. Conditions use `airflow.AllOf` and
   `airflow.AnyOf`, since Go has no operator overloading.
2. **Builders live in packages used as namespaces**: `schedule`, `mapper` and `python` under
   `github.com/apache/airflow/go-sdk`. They return the `airflow.Timetable`,
   `airflow.PartitionMapper` and `airflow.Trigger` interfaces, so `airflow` imports none of them.
   Each interface has one exported method that returns the value's serialized JSON, since a type
   in another package cannot implement an unexported method.
3. **Required builder arguments are positional; optional ones are a trailing options struct**, as
   `airflow.Dag(id, spec ...DagSpec)` takes its spec. A field is a pointer only when its zero value
   is a valid setting that differs from unset, such as `RunOffset *int`, as in the generated
   `DagSpec`. Otherwise the zero value means unset.
4. **`DagSpec.Schedule` changes from `string` to `airflow.Timetable`.** `schedule.Literal("@daily")`
   wraps a cron expression or preset and keeps today's meaning of a string schedule. This breaks
   `Schedule: "@daily"`, which is acceptable while the SDK is experimental, and lets the compiler
   reject any other type.
5. **`TaskSpec` gets `Inlets` and `Outlets`.**
6. **`airflow.AssetExtra` aliases `map[string]any`** for asset and event `extra`, so a plain map
   literal still fits. It is defined in `sdk` and re-exported by `airflow`.
7. **Outlet and triggering events are on `airflow.Context`; inlet events and the asset state store
   are on `sdk.Client`**, through a new `sdk.AssetClient` beside `TaskStateStoreClient`. The asset
   crosses into `sdk` as `sdk.AssetKey`, which every `airflow.AssetLike` returns. An alias passed
   to the asset state store returns an error, since its messages address an asset by name or URI.
   `sdk.AssetStateStore` mirrors `TaskStateStore` without `SetOption`, since the asset messages
   carry no retention.
8. **Inlet events take `sdk.InletEventQueryParams`**, which `airflow` re-exports with
   `airflow.Ascending` and `airflow.Descending`. `Order` replaces Python's `ascending` because its
   zero value, `Ascending`, is Python's default. `Limit: 0` means no limit. Durations are
   `time.Duration` and datetimes are `time.Time`.

## Signatures

```go
// package airflow
type AssetLike interface {                                 // Asset, AssetRef, AssetAlias
    AssetExpression
    AssetKey() sdk.AssetKey
}
type AssetExpression interface{ assetExpression() }       // AssetLike, or AllOf / AnyOf
type Timetable interface{ TimetableJSON() any }         // a cron or preset string, or {"__type", "__var"}
type PartitionMapper interface{ PartitionMapperJSON() map[string]any }  // {"__type", "__var"}
type Trigger interface{ TriggerJSON() map[string]any }                  // {"classpath", "kwargs", "queue"}

type Asset struct {
    Name, URI string                                        // one is required; the other defaults to it
    Group         string                                    // "" means "asset"
    Extra         AssetExtra
    Watchers      []AssetWatcher
    AccessControl *AssetAccessControl                       // nil takes Python's defaults
}
type AssetAccessControl struct {
    ProducerTeams []string                                  // default empty
    ConsumerTeams []string                                  // nil means unset
    AllowGlobal   *bool                                     // nil means true
}
type AssetRef struct{ Name, URI string }                   // exactly one is set
type AssetAlias struct{ Name, Group string }               // Group "" means "asset"
type AssetWatcher struct {
    Name    string
    Trigger Trigger                                         // must name a BaseEventTrigger
}
func AllOf(items ...AssetExpression) AssetExpression
func AnyOf(items ...AssetExpression) AssetExpression

type AssetExtra = sdk.AssetExtra
type DagSpec struct { Schedule Timetable ; /* ... */ }
type TaskSpec struct { Inlets, Outlets []AssetLike ; /* ... */ }

func (c Context) OutletEvents(asset AssetLike) *OutletEventAccessor
func (c Context) TriggeringAssetEvents(asset AssetLike) []sdk.TriggeringAssetEvent
type OutletEventAccessor struct{ /* unexported */ }
func (a *OutletEventAccessor) SetExtra(extra AssetExtra)
func (a *OutletEventAccessor) AddPartitions(keys ...string) error            // error on an alias
func (a *OutletEventAccessor) Add(asset AssetLike, extra AssetExtra) error  // error on a non-alias accessor or an alias argument

type InletEventQueryParams = sdk.InletEventQueryParams
const (
    Ascending  = sdk.Ascending
    Descending = sdk.Descending
)

// package sdk
type AssetExtra = map[string]any
type AssetKey struct{ Name, URI, AliasName string }
type AssetKeyer interface{ AssetKey() AssetKey }
type AssetClient interface {                              // embedded in Client
    InletEvents(asset AssetKeyer) InletEventAccessor
    AssetStateStore(asset AssetKeyer) AssetStateStore
}
type SortOrder int
const (
    Ascending SortOrder = iota
    Descending
)
type InletEventQueryParams struct {
    After, Before       time.Time                          // zero means unset
    Order               SortOrder
    Limit               int                                // 0 means no limit
    PartitionKey        string
    PartitionKeyPattern string
    Extra               map[string]string
}
type InletEventAccessor interface {
    Query(ctx context.Context, params ...InletEventQueryParams) ([]AssetEvent, error)
    // First returns the oldest event and Last the newest, or nil when there is none. Both
    // return an error when params sets Order or Limit.
    First(ctx context.Context, params ...InletEventQueryParams) (*AssetEvent, error)
    Last(ctx context.Context, params ...InletEventQueryParams) (*AssetEvent, error)
}
type AssetStateStore interface {
    Get(ctx context.Context, key string) (any, error)      // wraps AssetStateNotFound
    Set(ctx context.Context, key string, value any) error
    Delete(ctx context.Context, key string) error
    Clear(ctx context.Context) error
}
var AssetStateNotFound error
type AssetEvent struct {                                  // created_dagruns is not exposed
    ID             int
    Timestamp      time.Time
    Extra          AssetExtra
    Asset          AssetInfo                               // AssetInfo{Name, URI, Group string; Extra AssetExtra}
    SourceDagID    string
    SourceTaskID   string
    SourceRunID    string
    SourceMapIndex *int
    PartitionKey   string
}
type TriggeringAssetEvent struct {                        // from consumed_asset_events; no ID or asset Group
    Timestamp      time.Time
    Extra          AssetExtra
    Asset          AssetInfo                               // Group is ""
    SourceDagID    string
    SourceTaskID   string
    SourceRunID    string
    SourceMapIndex *int
    SourceAliases  []string
    PartitionKey   string
}

// package schedule
func Literal(expression string) airflow.Timetable           // cron expression or preset, in the Dag timezone
type RunImmediately struct {                              // zero value is Python's False; setting both
    Always bool                                             // fields is rejected. Python's None (10% of
    Within time.Duration                                    // the interval) is not exposed.
}
type CronOptions struct {
    Interval       time.Duration
    RunImmediately RunImmediately
}
func Cron(expression, timezone string, opts ...CronOptions) airflow.Timetable
func CronDataInterval(expression, timezone string) airflow.Timetable
func MultipleCron(expressions []string, timezone string, opts ...CronOptions) airflow.Timetable
type CronPartitionOptions struct {
    RunOffset      *int                                    // nil is Python's None
    RunImmediately RunImmediately
    KeyFormat      string                                  // "" is Python's default
}
func CronPartition(expression, timezone string, opts ...CronPartitionOptions) airflow.Timetable
type DeltaOptions struct{ Interval time.Duration }
func Delta(delta time.Duration, opts ...DeltaOptions) airflow.Timetable
func DeltaDataInterval(delta time.Duration) airflow.Timetable
type EventsOptions struct {
    RestrictToEvents, Presorted bool
    Description                 string
}
func Events(dates []time.Time, opts ...EventsOptions) airflow.Timetable
func Once() airflow.Timetable
func Continuous() airflow.Timetable
func None() airflow.Timetable
func PartitionedAtRuntime() airflow.Timetable
// BatchAssetEvents: nil takes Python's default; false is rejected for AND conditions and rollups.
type AssetOptions struct{ BatchAssetEvents *bool }
func Asset(condition airflow.AssetExpression, opts ...AssetOptions) airflow.Timetable
func AssetOrTime(condition airflow.AssetExpression, timetable airflow.Timetable, opts ...AssetOptions) airflow.Timetable
func AssetAndTime(condition airflow.AssetExpression, timetable airflow.Timetable) airflow.Timetable
type AssetMapper struct {
    Asset  airflow.AssetLike
    Mapper airflow.PartitionMapper
}
type PartitionedAssetOptions struct {
    DefaultMapper    airflow.PartitionMapper               // nil means mapper.Identity()
    Mappers          []AssetMapper
    BatchAssetEvents *bool
}
func PartitionedAsset(condition airflow.AssetExpression, opts ...PartitionedAssetOptions) airflow.Timetable

// package mapper
type Window interface{ window() }
type WaitPolicy interface{ waitPolicy() }
type Options struct{ MaxDownstreamKeys int }                // 0 means unset; every mapper takes it
type TemporalOptions struct {
    Options
    Timezone, InputFormat, OutputFormat string             // "" takes Python's default
}
type ProductOptions struct {
    Options
    Delimiter string                                       // "" means "|"
}
type RollupSpec struct {
    Options
    Upstream   airflow.PartitionMapper
    Window     Window
    WaitPolicy WaitPolicy                                  // nil means WaitForAll()
}
type FanOutSpec struct {
    Options
    Upstream   airflow.PartitionMapper
    Window     Window
    Downstream airflow.PartitionMapper                     // required for HourWindow and SegmentWindow
    // A window matches the upstream mapper's granularity, such as StartOfDay with DayWindow.
}
type Direction int
const (
    Forward Direction = iota
    Backward
)
type WindowOptions struct{ Direction Direction }
func Identity(opts ...Options) airflow.PartitionMapper
func FixedKey(downstreamKey string, opts ...Options) airflow.PartitionMapper
func AllowedKey(allowedKeys []string, opts ...Options) airflow.PartitionMapper
func StartOfHour(opts ...TemporalOptions) airflow.PartitionMapper
func StartOfDay(opts ...TemporalOptions) airflow.PartitionMapper
func StartOfWeek(opts ...TemporalOptions) airflow.PartitionMapper
func StartOfMonth(opts ...TemporalOptions) airflow.PartitionMapper
func StartOfQuarter(opts ...TemporalOptions) airflow.PartitionMapper
func StartOfYear(opts ...TemporalOptions) airflow.PartitionMapper
func Chain(mappers []airflow.PartitionMapper, opts ...Options) airflow.PartitionMapper  // two or more
func Product(mappers []airflow.PartitionMapper, opts ...ProductOptions) airflow.PartitionMapper  // two or more
func Rollup(spec RollupSpec) airflow.PartitionMapper
func FanOut(spec FanOutSpec) airflow.PartitionMapper
func HourWindow(opts ...WindowOptions) Window
func DayWindow(opts ...WindowOptions) Window
func WeekWindow(opts ...WindowOptions) Window
func MonthWindow(opts ...WindowOptions) Window
func QuarterWindow(opts ...WindowOptions) Window
func YearWindow(opts ...WindowOptions) Window
func SegmentWindow(segments []string) Window
func WaitForAll() WaitPolicy
func MinimumCount(n int) WaitPolicy                         // n != 0; negative counts from the expected total

// package python
type TriggerOptions struct{ Queue string }
func TimetableRef(path string, data map[string]any) airflow.Timetable
func PartitionMapperRef(path string, data map[string]any) airflow.PartitionMapper
func TriggerRef(path string, kwargs map[string]any, opts ...TriggerOptions) airflow.Trigger
```

## Example

The builder values are listed to show every option; a real Dag passes one timetable to `Schedule`.

```go
import (
    "errors"
    "time"

    "github.com/apache/airflow/go-sdk/airflow"
    "github.com/apache/airflow/go-sdk/mapper"
    "github.com/apache/airflow/go-sdk/python"
    "github.com/apache/airflow/go-sdk/schedule"
    "github.com/apache/airflow/go-sdk/sdk"
)

var (
    no, yes = false, true
    offset  = -1

    orders = airflow.Asset{
        Name:  "orders",
        URI:   "s3://lake/orders",
        Group: "sales",
        Extra: airflow.AssetExtra{"owner": "sales"},
        AccessControl: &airflow.AssetAccessControl{
            ProducerTeams: []string{"sales"},
            ConsumerTeams: []string{"finance"},
            AllowGlobal:   &no,
        },
        Watchers: []airflow.AssetWatcher{{
            Name: "orders_queue",
            Trigger: python.TriggerRef("my_plugin.triggers.OrdersQueueTrigger",
                map[string]any{"queue_name": "orders"}, python.TriggerOptions{Queue: "default"}),
        }},
    }
    customers    = airflow.AssetRef{Name: "customers"}
    rawFiles     = airflow.AssetRef{URI: "s3://lake/raw"}
    exportsAlias = airflow.AssetAlias{Name: "daily_exports", Group: "sales"}

    both   = airflow.AllOf(orders, customers)
    either = airflow.AnyOf(orders, airflow.AllOf(customers, rawFiles), exportsAlias)

    timetables = []airflow.Timetable{
        schedule.Cron("0 3 * * *", "Europe/Amsterdam", schedule.CronOptions{
            Interval: time.Hour, RunImmediately: schedule.RunImmediately{Within: 2 * time.Hour}}),
        schedule.CronDataInterval("@daily", "UTC"),
        schedule.MultipleCron([]string{"0 3 * * 1-5", "0 9 * * 6"}, "UTC", schedule.CronOptions{
            RunImmediately: schedule.RunImmediately{Always: true}}),
        schedule.CronPartition("0 0 * * *", "UTC", schedule.CronPartitionOptions{
            RunOffset: &offset, RunImmediately: schedule.RunImmediately{Always: true}, KeyFormat: "%Y-%m-%d"}),
        schedule.Delta(6*time.Hour, schedule.DeltaOptions{Interval: time.Hour}),
        schedule.DeltaDataInterval(24 * time.Hour),
        schedule.Events([]time.Time{time.Date(2026, 12, 24, 0, 0, 0, 0, time.UTC)},
            schedule.EventsOptions{RestrictToEvents: true, Presorted: true, Description: "Holidays"}),
        schedule.Once(),
        schedule.Continuous(),
        schedule.None(),
        schedule.PartitionedAtRuntime(),
        schedule.Asset(either, schedule.AssetOptions{BatchAssetEvents: &yes}),
        schedule.AssetOrTime(orders, schedule.Cron("0 3 * * *", "UTC"), schedule.AssetOptions{BatchAssetEvents: &no}),
        schedule.AssetAndTime(both, schedule.Cron("0 3 * * *", "UTC")),
        schedule.PartitionedAsset(both, schedule.PartitionedAssetOptions{
            DefaultMapper: mapper.StartOfDay(mapper.TemporalOptions{
                Timezone: "Europe/Amsterdam", InputFormat: "%Y-%m-%d", OutputFormat: "%Y-%m-%d"}),
            Mappers: []schedule.AssetMapper{
                {Asset: customers, Mapper: mapper.Chain(
                    []airflow.PartitionMapper{mapper.StartOfHour(), mapper.FixedKey("all")},
                    mapper.Options{MaxDownstreamKeys: 10})},
                {Asset: orders, Mapper: mapper.Product(
                    []airflow.PartitionMapper{mapper.Identity(), mapper.AllowedKey([]string{"eu", "us"})},
                    mapper.ProductOptions{Delimiter: "|", Options: mapper.Options{MaxDownstreamKeys: 4}})},
            },
            BatchAssetEvents: &yes,
        }),
        python.TimetableRef("my_plugin.timetables.Workday", map[string]any{"holidays": "us"}),
    }

    partitionMappers = []airflow.PartitionMapper{
        mapper.Chain([]airflow.PartitionMapper{mapper.StartOfWeek(), mapper.StartOfMonth()}),
        mapper.Rollup(mapper.RollupSpec{Upstream: mapper.StartOfHour(), Window: mapper.HourWindow()}),
        mapper.Rollup(mapper.RollupSpec{Upstream: mapper.StartOfDay(), Window: mapper.DayWindow(mapper.WindowOptions{Direction: mapper.Forward})}),
        mapper.Rollup(mapper.RollupSpec{Upstream: mapper.StartOfMonth(mapper.TemporalOptions{InputFormat: "%Y-%m-%d"}),
            Window: mapper.MonthWindow(), WaitPolicy: mapper.MinimumCount(-2)}),
        mapper.Rollup(mapper.RollupSpec{Upstream: mapper.FixedKey("all_regions"), Window: mapper.SegmentWindow([]string{"eu", "us"}), WaitPolicy: mapper.WaitForAll()}),
        mapper.FanOut(mapper.FanOutSpec{Upstream: mapper.StartOfWeek(),
            Window: mapper.WeekWindow(mapper.WindowOptions{Direction: mapper.Backward}), Options: mapper.Options{MaxDownstreamKeys: 7}}),
        mapper.FanOut(mapper.FanOutSpec{Upstream: mapper.StartOfQuarter(), Window: mapper.QuarterWindow()}),
        mapper.FanOut(mapper.FanOutSpec{Upstream: mapper.StartOfYear(), Window: mapper.YearWindow()}),
        mapper.FanOut(mapper.FanOutSpec{Upstream: mapper.Identity(), Window: mapper.SegmentWindow([]string{"eu", "us"}), Downstream: mapper.Identity()}),
        python.PartitionMapperRef("my_plugin.mappers.FiscalQuarter", map[string]any{"start_month": 4}),
    }
)

func newDags() []*airflow.DagRef {
    cronDag := airflow.Dag("cron_report", airflow.DagSpec{Schedule: schedule.Literal("0 3 * * *"), Queue: "golang"})
    cronDag.Task(load)
    dag := airflow.Dag("daily_report", airflow.DagSpec{Schedule: schedule.Asset(both), Queue: "golang"})
    dag.Task(load, airflow.TaskSpec{
        Inlets:  []airflow.AssetLike{orders, customers, exportsAlias},
        Outlets: []airflow.AssetLike{orders, exportsAlias},
    })
    return []*airflow.DagRef{cronDag, dag}
}

func load(actx airflow.Context) error {
    client := actx.Client()
    why := actx.TriggeringAssetEvents(orders)

    recent, err := client.InletEvents(orders).Query(actx, airflow.InletEventQueryParams{
        After:               time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
        Before:              time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC),
        Order:               airflow.Descending,
        Limit:               10,
        PartitionKey:        "2026-11-22",
        PartitionKeyPattern: "^2026-11-",
        Extra:               map[string]string{"region": "eu"},
    })
    if err != nil {
        return err
    }
    all, err := client.InletEvents(rawFiles).Query(actx, airflow.InletEventQueryParams{Order: airflow.Ascending})
    if err != nil {
        return err
    }
    oldest, err := client.InletEvents(customers).First(actx)
    if err != nil {
        return err
    }
    latest, err := client.InletEvents(exportsAlias).Last(actx, airflow.InletEventQueryParams{PartitionKey: "2026-11-22"})
    if err != nil {
        return err
    }

    sources := []string{}
    for _, e := range why {
        sources = append(sources, e.SourceAliases...)
    }
    actx.OutletEvents(orders).SetExtra(map[string]any{"rows": len(recent), "raw": len(all), "sources": sources})
    if err := actx.OutletEvents(orders).AddPartitions("2026-11-22"); err != nil {
        return err
    }
    export := airflow.Asset{Name: "export_2026_11_22", URI: "s3://lake/exports/2026-11-22"}
    if err := actx.OutletEvents(exportsAlias).Add(export, map[string]any{"rows": 10}); err != nil {
        return err
    }

    store := client.AssetStateStore(orders)
    mark := latest
    if mark == nil {
        mark = oldest
    }
    if mark != nil {
        if err := store.Set(actx, "watermark", mark.Timestamp.Format(time.RFC3339)); err != nil {
            return err
        }
    }
    if _, err := store.Get(actx, "watermark"); err != nil && !errors.Is(err, sdk.AssetStateNotFound) {
        return err
    }
    if err := store.Delete(actx, "watermark"); err != nil {
        return err
    }
    return store.Clear(actx)
}
```

## Consequences

- Every existing `Schedule: "..."` must become `Schedule: schedule.Literal("...")`. The schema that
  `internal/genspec` injects for `schedule`, its test and the field's doc comment change with it.
- Every fake `sdk.Client`, such as in tests, gains the `AssetClient` methods.
- `Add` on a non-alias accessor returns an error, where Python ignores it.
- The builders must change whenever a Python built-in's `serialize()` changes; the golden fixtures
  from core ADR-0019 catch that in CI.
