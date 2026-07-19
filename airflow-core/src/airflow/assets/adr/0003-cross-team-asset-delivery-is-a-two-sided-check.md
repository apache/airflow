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

# 3. Cross-team asset event delivery is a two-sided, default-open check

Date: 2026-07-19

## Status

Accepted

## Context

In a multi-team deployment (`[core] multi_team = True`) an asset can be produced
by a task in one team and consumed by a Dag in another. Asset events are the
wiring between otherwise-isolated teams, so *which* consumers an event reaches
becomes an access-control question, not just a scheduling one. Two independent
parties have a legitimate say:

- the **consumer** decides which producers it is willing to be woken by
  (producer-side control — `allow_producer_teams` / `allow_global_producers`);
- the **producer** decides which consumers may receive its events
  (consumer-side control — `consumer_teams` / `allow_global` on
  `AssetAccessControl`, resolved for a task via its `TaskOutletAssetReference`).

`AssetManager._filter_dags_by_team` applies both. A consumer Dag is queued only
if the producer-side check *and* the consumer-side check both pass — a logical
**AND**. The controls are stored where each side owns them: the consumer's
`allow_producer_teams` / `allow_global_producers` live on the
schedule-reference row (`DagScheduleAssetReference`), deliberately **not** on the
shared `AssetModel`, so one team editing its schedule cannot rewrite another
team's asset. Events produced through the REST API carry no producing-Dag
bundle, so their source teams (and consumer-team allowances) must be passed in
explicitly (`api_user_teams`, `api_allow_consumer_teams`,
`api_allow_global_consumers`) rather than inferred.

The whole filter is also **default-open**: with `multi_team` off it is a no-op,
and teamless sources/consumers and the `allow_global*` defaults preserve
today's single-team behaviour. This lets the feature ship without changing
delivery for existing deployments — but it also means every branch has to keep
the permissive default exactly right, because a subtle inversion either leaks
events across a team boundary or silently starves legitimate consumers.

## Decision

Cross-team asset event delivery is decided by a **two-sided team check**:

- Both the producer-side and consumer-side checks must pass (logical AND) for a
  consumer Dag to be queued. Neither side alone is sufficient.
- Each side's control is stored where that side owns it: the consumer's
  `allow_producer_teams` / `allow_global_producers` on the schedule-reference
  row, the producer's `consumer_teams` / `allow_global` via the asset's
  `AssetAccessControl` (resolved through the outlet reference for a task). A
  team must not be able to change another team's control by editing a shared
  row.
- The filter is **default-open and gated on `multi_team`**: disabled entirely
  when the flag is off, and preserving today's delivery for teamless
  sources/consumers and the `allow_global*` defaults. Enabling multi-team must
  not silently drop events that flow today under equivalent configuration.
- **API-produced events supply their teams explicitly.** When there is no
  producing task (`source_is_api`), source teams and consumer-team allowances
  come from the caller, never guessed from a Dag bundle.

## Consequences

- Teams get mutual, self-owned control over cross-team asset wiring without one
  team being able to override another's policy.
- The permissive defaults keep single-team and teamless deployments behaving
  exactly as before, so multi-team can be adopted incrementally.
- Every change to `_filter_dags_by_team` carries an outsized correctness burden:
  it sits directly on both a security boundary (event leakage) and an
  availability one (starved consumers), and must be tested for both directions
  and for the flag-off / teamless defaults.

A change **violates** this decision when it:

- queues a consumer on a producer-side *or* consumer-side pass alone, collapsing
  the AND into an OR;
- moves a consumer's `allow_producer_teams` (or a producer's consumer control)
  onto the shared `AssetModel`, letting one team rewrite another team's policy;
- changes the default so that `multi_team` off, a teamless source/consumer, or
  the `allow_global*` defaults start dropping (or start leaking) events relative
  to current behaviour;
- infers an API-produced event's teams from a bundle instead of requiring them
  to be passed explicitly.

## Evidence

- #66168 — "Update `register_asset_change` to filter Dags based on teams":
  introduces producer-team filtering of consumer Dags in the event path.
- #68025 — "Apply consumer team filtering": adds the consumer-side half, making
  delivery a two-sided check; #68242 ("Fix team consumer asset filtering") then
  corrects that half.
- #66487 — "Store `allow_producer_teams` in `dag_schedule_asset_reference`
  instead of `asset`": puts each side's control where that side owns it, off the
  shared asset row.
- #67251 — "Add `allow_global` option to asset access control": the default-open
  global allowance that keeps teamless delivery working.
- #66367 — "Updated `create_asset_event` endpoint to pass the user teams":
  API-produced events carry their teams explicitly rather than inferring them.
